package com.bjvca.dmp.adx.streaming

import java.lang
import java.text.SimpleDateFormat
import java.util.Date
import com.bjvca.commonutils.ConfUtils
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, KafkaUtils}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * adx 统计曝光 的实时程序
 */
object BgAdxStreamingMain extends Logging with Serializable {
  def main(args: Array[String]): Unit = {

    //        val confUtil = new ConfUtils("application.conf")
    val confUtil = new ConfUtils("线上application.conf")

    val conf = new SparkConf()
      .setMaster(confUtil.adxStreamingSparkMaster)
      .setAppName("StreamingProcessMain")
      .set("spark.streaming.stopGracefullyOnShutdown", "true")

    val sparkStreaming = new StreamingContext(conf, Seconds(confUtil.adxStreamingSparkDuration))

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> s" ${confUtil.adxStreamingKafkaHost}:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "spark-adx-bg",
      "auto.offset.reset" -> "earliest", // earliest latest
      "enable.auto.commit" -> (false: lang.Boolean)
    )

    val topics = Array("sltadxtopic")
    val stream = KafkaUtils.createDirectStream[String, String](
      sparkStreaming,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    stream
      .foreachRDD(rdd => {
        logWarning("批开始")

        val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

        val sparkSql = SparkSession.builder()
          .config(rdd.sparkContext.getConf)
          .getOrCreate()

        // 获得当天的时间
        confUtil.nowTime = new SimpleDateFormat("yyyy-MM-dd").format(new Date().getTime)

        //          confUtil.nowTime = "2019-12-25"

        // 处理数据
        val cleanedRDD = BgReducing.cleanData(rdd, confUtil)

        if (cleanedRDD.isEmpty()) {
          logWarning("empty rdd")
        }
        else {
          // redis
          BgReducing.saveToRedis(cleanedRDD, confUtil)
          // mysql
          BgReducing.saveToMySql(sparkSql, cleanedRDD, confUtil)
          // es
          BgReducing.saveToES(sparkSql, cleanedRDD, confUtil)
          // 释放缓存表
          BgReducing.unCacheTables(sparkSql)
        }

        stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)

      })

    sparkStreaming.start()

    sparkStreaming.awaitTermination()

    logWarning("AdxStreamingMain over")
  }
}