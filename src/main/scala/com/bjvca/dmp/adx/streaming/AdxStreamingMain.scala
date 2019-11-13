package com.bjvca.dmp.adx.streaming

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent

object AdxStreamingMain {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setMaster("local[4]")
      .setAppName("StreamingProcessMain")
    val spark = new StreamingContext(conf, Seconds(10))


    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> " 192.168.1.130:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "mac3",
      "auto.offset.reset" -> "earliest", // earliest latest
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array("sltadxlog-adx-bg")
    val stream = KafkaUtils.createDirectStream[String, String](
      spark,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    stream.map(record => record.value)
      .foreachRDD(Reducing.reduceRDD)


    println("over")

    spark.start()

    spark.awaitTermination()


  }

}
