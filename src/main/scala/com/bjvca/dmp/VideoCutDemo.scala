package com.bjvca.dmp

import com.alibaba.fastjson.{JSON, JSONArray}
import org.apache.parquet.schema.Types.ListBuilder
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.aggregate.Max

import scala.collection.mutable.ListBuffer

object VideoCutDemo {

  def main(args: Array[String]): Unit = {


    val spark = SparkSession.builder()
      .appName("VideoCut")
      .master("local")
      .getOrCreate()


    import spark.implicits._
    val options = Map("url" -> s"jdbc:mysql://192.168.1.45:3306/adx?characterEncoding=utf-8&useSSL=false",
      "driver" -> "com.mysql.jdbc.Driver",
      "dbtable" -> "spark_video_cut",
      "user" -> "root",
      "password" -> "123456")

    val mysqlRDD = spark.read.format("jdbc")
      .options(options)
      .load()
      .select($"video_id", $"media_name", $"ad_seat_b_time", $"ad_seat_e_time",
        $"class_type_id", $"class3_name")
      .toJSON
      .rdd

    val reduced = mysqlRDD.map(x => {
      val jsonArray = new JSONArray()
      val key = JSON.parseObject(x).get("video_id").toString
      jsonArray.add(x)
      (key, jsonArray)
    })
      .reduceByKey((x, y) => {
        for (i <- 0 until y.size()) {
          x.add(y.get(i))
        }
        x
      })
      .map(videoList=>{

      })



  }



}
