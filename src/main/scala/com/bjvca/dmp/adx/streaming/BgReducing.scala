package com.bjvca.dmp.adx.streaming

import java.sql.{Connection, DriverManager, PreparedStatement}

import com.alibaba.fastjson.JSON
import com.bjvca.commonutils.{ConfUtils, JedisPools, TableRegister}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SparkSession}

object BgReducing extends Logging with Serializable {

  def cleanData(rdd: RDD[ConsumerRecord[String, String]], confUtil: ConfUtils): RDD[(String, Int)] = {

    val result = rdd.filter(line => {

      try {

        JSON.parseObject(line.value()).getString("sltcustomtopic").equals("sltadxlog-adx-bg")
      } catch {
        case exception: Exception => false
      }

    })
      .filter(_ != false)
      .map(line => {
        (JSON.parseObject(line.value()).getString("dealid"), 1)
      })
      .reduceByKey((x, y) => x + y)

    logWarning("OriginalRDD completed")

    result
  }

  def saveToRedis(rdd: RDD[(String, Int)], confUtil: ConfUtils) = {

    rdd.foreachPartition(part => {

      val jedis = new JedisPools(confUtil.adxStreamingRedisHost,
        confUtil.adxStreamingRedisPassword, confUtil.adxStreamingRedisDB).getJedis()

      part.foreach(x => {
        jedis.incrBy(s"adxhappen:${x._1}", x._2)
      })

      jedis.close()

    })

    logWarning("CountToRedis completed")
  }

  def saveToMySql(sparkSql: SparkSession, rdd: RDD[(String, Int)], confUtil: ConfUtils) = {

    // 注册mysql的spark_streaming_bg表
    TableRegister.registMysqlTable(sparkSql, confUtil.adxStreamingMysqlHost, "adx",
      confUtil.adxStreamingMysqlUser, confUtil.adxStreamingMysqlPassword,
      "spark_streaming_bg", "spark_streaming_bg_mysql")

    import sparkSql.implicits._
    rdd.map(x => {
      (x._1, confUtil.nowTime, x._2)
    })
      .toDF("dealid", "now_time", "total")
      .createOrReplaceTempView("spark_streaming_bg_rdd")

    val sparkStreamingBg = sparkSql.sql(
      """
        |SELECT
        |a.dealid as dealid,
        |a.now_time as now_time,
        |(a.total+IFNULL(b.total,0)) as total
        |FROM
        |spark_streaming_bg_rdd a
        |LEFT JOIN
        |spark_streaming_bg_mysql b
        |ON a.dealid=b.dealid and a.now_time=b.now_time
        |""".stripMargin)

    sparkStreamingBg.createOrReplaceTempView("spark_streaming_bg")

    sparkSql.sql("cache table spark_streaming_bg")

    sparkStreamingBg.foreachPartition(iterator => {
      var conn: Connection = null
      var ps: PreparedStatement = null
      val updateSQL =
        """INSERT INTO spark_streaming_bg(`dealid`,`now_time`,`total`)
          |VALUES(?,?,?)
          |ON DUPLICATE KEY UPDATE
          |`total`=?
          |""".stripMargin

      try {
        conn = DriverManager.getConnection(s"jdbc:mysql://${confUtil.adxStreamingMysqlHost}:3306/adx?characterEncoding=utf-8&useSSL=false",
          confUtil.adxStreamingMysqlUser, confUtil.adxStreamingMysqlPassword)
        conn.setAutoCommit(false)
        ps = conn.prepareStatement(updateSQL)
        var row = 0
        iterator.foreach(it => {

          ps.setString(1, it.getAs[String]("dealid"))
          ps.setString(2, it.getAs[String]("now_time"))
          ps.setInt(3, it.getAs[Int]("total"))
          ps.setInt(4, it.getAs[Int]("total"))

          ps.addBatch()
          row = row + 1
          if (row % 1000 == 0) {
            ps.executeBatch()
            row = 0
          }
        })
        if (row > 0)
          ps.executeBatch()
        conn.commit()

      } catch {
        case e: Exception => e.printStackTrace()
      } finally {
        if (ps != null) {
          ps.close()
        }
        if (conn != null) {
          conn.close()
        }
      }
    })

    logWarning("SaveToMysql completed")


  }

  def saveToES(sparkSql: SparkSession, rdd: RDD[(String, Int)], confUtil: ConfUtils) = {

    try {
      // 注册es的vcaadx表
      TableRegister.registEsTable(sparkSql, confUtil.adxStreamingEsHost, "9200",
        confUtil.adxStreamingEsUser, confUtil.adxStreamingEsPassword,
        "vcaadx/test", "vcaadx_es")

      import sparkSql.implicits._
      val resultList = sparkSql.sql(
        """
          |SELECT
          |a.dealid
          |FROM
          |spark_streaming_bg a
          |JOIN vcaadx_es b
          |ON a.dealid=b.dealid and a.now_time=b.now_time
          |WHERE
          |a.total > (b.dealidnum-30)
          |""".stripMargin)
        .map(x => {
          x.get(0).toString
        })
        .collectAsList()

      // 将消费flag更新到es
      import scala.collection.JavaConversions._
      SaveToEsUtils.updateFlagToES(confUtil, resultList)

      logWarning("SaveToEs completed")


    } catch {
      case e: Exception => {
        logWarning(e.toString)
        logWarning("链接Es异常")
      }
    }

  }

  def unCacheTables(sparksql: SparkSession) = {
    sparksql.sql("uncache table spark_streaming_bg")
  }

}