package com.bjvca.dmp.commonutils

import com.typesafe.config.ConfigFactory

class ConfUtils(confFile:String) extends Serializable {

  val load = ConfigFactory.load(confFile)

  // adx - streaming
  // spark
  val adxStreamingSparkMaster = load.getString("adx.streaming.spark.master")
  val adxStreamingSparkDuration = load.getLong("adx.streaming.spark.duration")

  // kafka
  val adxStreamingKafkaHost: String = load.getString("adx.streaming.kafka.host")
  val adxStreamingKafkaGroupid = load.getString("adx.streaming.kafka.groupid")

  // redis
  val adxStreamingRedisHost = load.getString("adx.streaming.redis.host")
  val adxStreamingRedisPassword = load.getString("adx.streaming.redis.password")
  val adxStreamingRedisDB = load.getInt("adx.streaming.redis.db")



}
