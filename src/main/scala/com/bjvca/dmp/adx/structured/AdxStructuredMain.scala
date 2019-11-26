package com.bjvca.dmp.adx.structured

import com.bjvca.dmp.commonutils.ConfUtils
import org.apache.spark.internal.Logging
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, ForeachWriter, SaveMode, SparkSession}
import redis.clients.jedis.Jedis

object AdxStructuredMain extends Logging {
  def main(args: Array[String]): Unit = {

    // 校验参数个数
    if (args.length != 1) {
      logError(
        """
          |com.bjvca.dmp.adx.structured.AdxStructuredMain
          |参数：
          |dmpConfFile
          |""".stripMargin
      )
      sys.exit(1)
    }

    val Array(dmpConfFile) = args

    val confUtil = new ConfUtils(dmpConfFile)

    val spark = SparkSession.builder()
      .appName(getClass.getName)
      .master(confUtil.adxStreamingSparkMaster)
      .config("spark.sql.streaming.streamingQueryListeners", "net.heartsavior.spark.KafkaOffsetCommitterListener")
      .config("spark.redis.host", confUtil.adxStreamingRedisHost)
      .config("spark.redis.port", "6379")
      .config("spark.redis.auth", confUtil.adxStreamingRedisPassword)
      .getOrCreate()

    import spark.implicits._
    import org.apache.spark.sql.functions._

    val df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", s"${confUtil.adxStreamingKafkaHost}:9092")
      .option("subscribe", "sltadxlog-adx-bg")
      .option("startingOffsets", "earliest")
      .load()

    val schema = StructType(Seq(
      StructField("video_id", StringType, true),
      StructField("adseat_id", StringType, true),
      StructField("drama_id", StringType, true),
      StructField("os", StringType, true),
      StructField("class_id", StringType, true),
      StructField("vis_id", StringType, true),
      StructField("pkg_id", StringType, true),
      StructField("media_channel_id", StringType, true),
      StructField("act_id", StringType, true),
      StructField("ad_id", StringType, true),
      StructField("dsp_id", StringType, true),
      StructField("dealid", StringType, true),
      StructField("order_id", StringType, true),
      StructField("main_order_id", StringType, true),
      StructField("advertiser_id", StringType, true),
      StructField("tag_id", StringType, true),
      StructField("adx_material_id", StringType, true)
    ))

    val result = df.selectExpr("CAST(topic AS STRING)", "CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String, String)]
      //      .filter($"topic" === "sltadxlog-adx-bg")
      .select(from_json($"value", schema = schema).as("data"))
      .select("data.dealid")

    val query = result.writeStream
      .outputMode("append")
      .queryName("table")
      .format("console")
      .start()

    query.awaitTermination()

    spark.stop()

    logWarning("AdxStreamingMain over")

  }

}