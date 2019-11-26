package com.bjvca.dmp.adx.streaming

import com.bjvca.dmp.commonutils.ConfUtils
import com.bjvca.dmp.utils.KafkaOffectCommiterListener
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, KafkaUtils}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object BgAdxStreamingMain extends Logging {
  def main(args: Array[String]): Unit = {

//    val confUtil = new ConfUtils("application.conf")
    val confUtil = new ConfUtils("线上application.conf")

    val conf = new SparkConf()
      .setMaster(confUtil.adxStreamingSparkMaster)
      .setAppName("StreamingProcessMain")
      .set("spark.streaming.stopGracefullyOnShutdown", "true")

    val spark = new StreamingContext(conf, Seconds(confUtil.adxStreamingSparkDuration))
    spark.addStreamingListener(new KafkaOffectCommiterListener)

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> s" ${confUtil.adxStreamingKafkaHost}:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "spark-adx-bg",
      "auto.offset.reset" -> "earliest", // earliest latest
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array("sltadxtopic")
    val stream = KafkaUtils.createDirectStream[String, String](
      spark,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    stream
      .foreachRDD(rdd => {
        logWarning("批开始")

        val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

        BgAdxStreamingReducing.countDealID(rdd, confUtil)

        stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)

      })

    spark.start()

    spark.awaitTermination()

    logWarning("AdxStreamingMain over")

  }
}