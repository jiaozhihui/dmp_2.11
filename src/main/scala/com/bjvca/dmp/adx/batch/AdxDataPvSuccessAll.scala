package com.bjvca.dmp.adx.batch

import java.sql.{Connection, DriverManager, PreparedStatement}
import java.text.SimpleDateFormat
import java.util.Date

import com.bjvca.dmp.commonutils.{ConfUtils, TableRegister}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

object AdxDataPvSuccessAll extends Logging {
  def getRpt(): Unit = {

    logWarning("AdxDataPvSuccess开始运行")

//    val confUtil = new ConfUtils("application.conf")
            val confUtil = new ConfUtils("线上application.conf")

    confUtil.nowTime = new SimpleDateFormat("yyyyMMdd").format(new Date().getTime - 86400L * 1000)
    val nowMouth = confUtil.nowTime.substring(0, 6)

    val sparkSql = SparkSession.builder()
      .appName("AdxDataPvSuccess")
      .master(confUtil.adxStreamingSparkMaster)
      .getOrCreate()

    // 注册mysql的ssp_tag表
    TableRegister.registMysqlTable(sparkSql, confUtil.adxStreamingMysqlHost, "adx",
      confUtil.adxStreamingMysqlUser, confUtil.adxStreamingMysqlPassword,
      "ssp_tag", "ssp_tag_mysql")

    // 注册mysql的adx_order_dealid表
    TableRegister.registMysqlTable(sparkSql, confUtil.adxStreamingMysqlHost, "adx",
      confUtil.adxStreamingMysqlUser, confUtil.adxStreamingMysqlPassword,
      "adx_order_dealid", "adx_order_dealid_mysql")

    import sparkSql.implicits._
    val originalDF = sparkSql.read.json(s"hdfs://${confUtil.adxBatchHDFSHost}/logsltadxlog-adxtodsp-request/${nowMouth}.log")

    originalDF
      .filter($"isHaveAd" === 1)
      .createOrReplaceTempView("adx_data_pv_success_hdfs")

    sparkSql.sql(
      """
        |SELECT
        |FinalAdobj.dealid as dealid,
        |CONCAT(left(`local-month`,4),"-",right(`local-month`,2),"-",first(`local-day`)) as daytime,
        |FIRST(media_channel_id) as media_channel_id,
        |get_json_object(FIRST(RedisadInfo),'$.tag_id') as tag_id,
        |SUBSTRING_INDEX(get_json_object(FIRST(RedisadInfo),'$.tag_id'),'_',-1) as tag_id_channel,
        |get_json_object(FIRST(RedisadInfo),'$.adx_material_id') as adx_material_id,
        |get_json_object(FIRST(RedisadInfo),'$.dsp_id') as dsp_id,
        |get_json_object(FIRST(RedisadInfo),'$.advertiser_id') as advertiser_id,
        |sum(1) as total
        |FROM adx_data_pv_success_hdfs
        |GROUP BY dealid,`local-month`,`local-day`
        |""".stripMargin).createOrReplaceTempView("adx_data_pv_success_original")

        sparkSql.sql(
          """
            |SELECT
            |a.dealid,
            |a.daytime,
            |c.order_id,
            |c.main_order_id,
            |c.is_compensate,
            |IFNULL(b.media_id,"000") AS media_id,
            |a.media_channel_id,
            |a.tag_id,
            |a.tag_id_channel,
            |a.adx_material_id,
            |a.dsp_id,
            |a.advertiser_id,
            |a.total
            |FROM
            |adx_data_pv_success_original a LEFT JOIN ssp_tag_mysql b
            |ON a.media_channel_id = b.channel_id
            |LEFT JOIN adx_order_dealid_mysql c
            |ON a.dealid = c.dealid
            |""".stripMargin)
          .foreachPartition(iterator => {
            var conn: Connection = null
            var ps: PreparedStatement = null
            val updateSQL =
              """INSERT INTO adx_data_pv_success(dealid, `daytime`, order_id, main_order_id, is_compensate,
                |media_id, media_channel_id, tag_id, tag_id_channel, adx_material_id,
                |dsp_id, advertiser_id, `total`)
                |VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
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
                ps.setString(3, it.getAs[String]("order_id"))
                ps.setString(4, it.getAs[String]("main_order_id"))
                ps.setInt(5, it.getAs[Int]("is_compensate"))
                ps.setString(6, it.getAs[String]("media_id"))
                ps.setString(7, it.getAs[String]("media_channel_id"))
                ps.setString(8, it.getAs[String]("tag_id"))
                ps.setString(9, it.getAs[String]("tag_id_channel"))
                ps.setString(10, it.getAs[String]("adx_material_id"))
                ps.setString(11, it.getAs[String]("dsp_id"))
                ps.setString(12, it.getAs[String]("advertiser_id"))
                ps.setInt(13, it.getAs[Long]("total").intValue())
                ps.setInt(14, it.getAs[Long]("total").intValue())

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

    sparkSql.stop()

  }
}