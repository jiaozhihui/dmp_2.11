package com.bjvca.dmp.adx.structured

import com.alibaba.fastjson.JSON
import com.bjvca.commonutils.{ConfUtils, JedisPools}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD


object AdxStructuredReducing extends Logging {


  def countDealID(rdd: RDD[ConsumerRecord[String, String]], confUtils: ConfUtils) = {

    if (rdd.isEmpty()) {
      logWarning("empty rdd")
    }
    else {

      rdd
        .map(line => {
          (JSON.parseObject(line.value()).getString("dealid"), 1)
        })
        .reduceByKey((x, y) => x + y)
        .foreachPartition(part => {

          val jedis = new JedisPools(confUtils.adxStreamingRedisHost,
            confUtils.adxStreamingRedisPassword, confUtils.adxStreamingRedisDB).getJedis()

          part.foreach(x => {
            jedis.incrBy(s"adxhappen:${x._1}", x._2)
          })

          jedis.close()


        })

      logWarning("rdd completed")

    }
  }


}
