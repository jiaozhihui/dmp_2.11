package com.bjvca.dmp.adx.batch

import java.sql.{Connection, DriverManager, PreparedStatement}
import java.text.SimpleDateFormat
import java.util.Date
import com.bjvca.commonutils.{ConfUtils, TableRegister}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

object AdxDataPvSuccess extends Logging {
  def getRpt(args: Array[String]): Unit = {

    logWarning("AdxDataPvSuccess开始运行")

    val confUtil = new ConfUtils("application.conf")
    //        val confUtil = new ConfUtils("线上application.conf")

    confUtil.nowTime = new SimpleDateFormat("yyyyMMdd").format(new Date().getTime - 86400L * 1000)
    val nowMouth = confUtil.nowTime.substring(0, 6)
    //    val nowDay = confUtil.nowTime.substring(6, 8)
    val nowDay = "09"

    val sparkSql = SparkSession.builder()
      .appName("AdxDataPvSuccess")
      .master(confUtil.adxStreamingSparkMaster)
      .getOrCreate()

    // 注册mysql的spark_streaming_bg表
    TableRegister.registMysqlTable(sparkSql, confUtil.adxStreamingMysqlHost, "adx",
      confUtil.adxStreamingMysqlUser, confUtil.adxStreamingMysqlPassword,
      "ssp_tag", "ssp_tag_mysql")

    import sparkSql.implicits._
    val originalDF = sparkSql.read.json(s"hdfs://${confUtil.adxBatchHDFSHost}/logsltadxlog-adxtodsp-request/${nowMouth}.log")

    originalDF.filter($"local-day" === nowDay)
      .filter($"isHaveAd" === 1)
      .createOrReplaceTempView("adx_data_pv_success_hdfs")

    sparkSql.sql(
      """
        |SELECT
        |FinalAdobj.dealid as dealid,
        |CONCAT(first(`local-month`),first(`local-day`)) as daytime,
        |FIRST(media_channel_id) as media_channel_id,
        |SUBSTRING_INDEX(get_json_object(FIRST(RedisadInfo),'$.tag_id'),'_',-1) as tag_id_channel,
        |get_json_object(FIRST(RedisadInfo),'$.adx_material_id') as adx_material_id,
        |get_json_object(FIRST(RedisadInfo),'$.dsp_id') as dsp_id,
        |get_json_object(FIRST(RedisadInfo),'$.advertiser_id') as advertiser_id,
        |sum(1) as total
        |FROM adx_data_pv_success_hdfs
        |GROUP BY dealid
        |""".stripMargin).createOrReplaceTempView("adx_data_pv_success_original")

        sparkSql.sql(
          """
            |SELECT
            |a.dealid,
            |a.daytime,
            |IFNULL(b.media_id,"000") AS media_id,
            |a.media_channel_id,
            |a.tag_id_channel,
            |a.adx_material_id,
            |a.dsp_id,
            |a.advertiser_id,
            |a.total
            |FROM
            |adx_data_pv_success_original a LEFT JOIN ssp_tag_mysql b
            |ON a.media_channel_id = b.channel_id
            |""".stripMargin)
          .foreachPartition(iterator => {
            var conn: Connection = null
            var ps: PreparedStatement = null
            val updateSQL =
              """INSERT INTO adx_data_pv_success(dealid, `daytime`, media_id, media_channel_id, tag_id_channel, adx_material_id, dsp_id, advertiser_id, `total`)
                |VALUES(?,?,?,?,?,?,?,?,?)
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
                ps.setString(2, it.getAs[String]("daytime"))
                ps.setString(3, it.getAs[String]("media_id"))
                ps.setString(4, it.getAs[String]("media_channel_id"))
                ps.setString(5, it.getAs[String]("tag_id_channel"))
                ps.setString(6, it.getAs[String]("adx_material_id"))
                ps.setString(7, it.getAs[String]("dsp_id"))
                ps.setString(8, it.getAs[String]("advertiser_id"))
                ps.setInt(9, it.getAs[Long]("total").intValue())
                ps.setInt(10, it.getAs[Long]("total").intValue())

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

        logWarning("写入adx_data_pv_success成功")

  }
}