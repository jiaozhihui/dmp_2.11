package com.bjvca.dmp.adx.batch

import java.sql.{Connection, DriverManager, PreparedStatement}
import java.text.SimpleDateFormat
import java.util.Date

import com.bjvca.dmp.commonutils.{ConfUtils, TableRegister}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

object AdxDataPvFailAll extends Logging {
  def getRpt(): Unit = {

    logWarning("AdxDataPvFail开始运行")

//    val confUtil = new ConfUtils("application.conf")
            val confUtil = new ConfUtils("线上application.conf")

    confUtil.nowTime = new SimpleDateFormat("yyyyMMdd").format(new Date().getTime - 86400L * 1000)
    val nowMouth = confUtil.nowTime.substring(0, 6)

    val sparkSql = SparkSession.builder()
      .appName("AdxDataPvFail")
      .master(confUtil.adxStreamingSparkMaster)
      .getOrCreate()

    // 注册mysql的spark_streaming_bg表
    TableRegister.registMysqlTable(sparkSql, confUtil.adxStreamingMysqlHost, "adx",
      confUtil.adxStreamingMysqlUser, confUtil.adxStreamingMysqlPassword,
      "ssp_tag", "ssp_tag_mysql")

    import sparkSql.implicits._
    val originalDF = sparkSql.read.json(s"hdfs://${confUtil.adxBatchHDFSHost}/logsltadxlog-adxtodsp-request/${nowMouth}.log")

    originalDF
      .filter($"isHaveAd" === 0)
      .createOrReplaceTempView("adx_data_pv_fail_hdfs")

    sparkSql.sql(
      """
        |SELECT
        |SUBSTRING_INDEX(DSPName,'_',-1) as dsp_id,
        |CONCAT(`local-month`,`local-day`) as daytime,
        |FIRST(media_channel_id) as media_channel_id,
        |SUBSTRING_INDEX(FIRST(options.body.imp[0].tagid),'_',-1) as tag_id_channel,
        |IFNULL(SUM(isReturn),0) as return,
        |IFNULL(SUM(isTimeOut),0) as timeout,
        |IFNULL(SUM(1),0) as total
        |FROM adx_data_pv_fail_hdfs
        |GROUP BY dsp_id,`local-month`,`local-day`
        |""".stripMargin).createOrReplaceTempView("adx_data_pv_fail_original")

    sparkSql.sql(
      """
        |SELECT
        |a.dsp_id,
        |a.daytime,
        |IFNULL(b.media_id,"000") AS media_id,
        |a.media_channel_id,
        |a.tag_id_channel,
        |a.return,
        |a.timeout,
        |total-return-timeout as else_error,
        |total
        |FROM
        |adx_data_pv_fail_original a LEFT JOIN ssp_tag_mysql b
        |ON a.media_channel_id = b.channel_id
        |""".stripMargin)
      .foreachPartition(iterator => {
        var conn: Connection = null
        var ps: PreparedStatement = null
        val updateSQL =
          """INSERT INTO adx_data_pv_fail(dsp_id, `daytime`, media_id, media_channel_id, tag_id_channel, `return`, `timeout`, `else_error`, `total`)
            |VALUES(?,?,?,?,?,?,?,?,?)
            |ON DUPLICATE KEY UPDATE
            |`return`=?,
            |`timeout`=?,
            |`else_error`=?,
            |`total`=?
            |""".stripMargin

        try {
          conn = DriverManager.getConnection(s"jdbc:mysql://${confUtil.adxStreamingMysqlHost}:3306/adx?characterEncoding=utf-8&useSSL=false",
            confUtil.adxStreamingMysqlUser, confUtil.adxStreamingMysqlPassword)
          conn.setAutoCommit(false)
          ps = conn.prepareStatement(updateSQL)
          var row = 0
          iterator.foreach(it => {

            ps.setString(1, it.getAs[String]("dsp_id"))
            ps.setString(2, it.getAs[String]("daytime"))
            ps.setString(3, it.getAs[String]("media_id"))
            ps.setString(4, it.getAs[String]("media_channel_id"))
            ps.setString(5, it.getAs[String]("tag_id_channel"))
            ps.setInt(6, it.getAs[Long]("return").intValue())
            ps.setInt(7, it.getAs[Long]("timeout").intValue())
            ps.setInt(8, it.getAs[Long]("else_error").intValue())
            ps.setInt(9, it.getAs[Long]("total").intValue())
            ps.setInt(10, it.getAs[Long]("return").intValue())
            ps.setInt(11, it.getAs[Long]("timeout").intValue())
            ps.setInt(12, it.getAs[Long]("else_error").intValue())
            ps.setInt(13, it.getAs[Long]("total").intValue())
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

    logWarning("写入adx_data_pv_fail成功")

  }
}