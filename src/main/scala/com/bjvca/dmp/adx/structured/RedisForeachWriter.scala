package com.bjvca.dmp.adx.structured

import com.bjvca.dmp.commonutils.{ConfUtils, JedisPools}
import org.apache.spark.sql.ForeachWriter
import redis.clients.jedis.Jedis

abstract class RedisForeachWriter(confUtils: ConfUtils) extends ForeachWriter[String] with Serializable {

  private val pools = new JedisPools(confUtils.adxStreamingRedisHost, confUtils.adxStreamingRedisPassword,
    confUtils.adxStreamingRedisDB)

  var jedis:Jedis

  override def open(partitionId: Long, epochId: Long): Boolean = {

    var jedis = pools.getJedis()
    true
  }

  override def process(value: String): Unit = {

    jedis.incrBy(s"adxhappen:${value}", 1)
  }

  override def close(errorOrNull: Throwable): Unit = {
    jedis.close
  }
}