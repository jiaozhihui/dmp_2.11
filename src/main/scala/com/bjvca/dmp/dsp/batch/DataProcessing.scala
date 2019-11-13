package com.bjvca.dmp.dsp.batch

import org.apache.spark.streaming._
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

object DataProcessing {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("DataProcessing")
      .master("local")
      .config("master","local")
      .getOrCreate()
    spark


  }

}
