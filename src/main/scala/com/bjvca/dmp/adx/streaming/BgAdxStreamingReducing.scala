package com.bjvca.dmp.adx.streaming

import com.alibaba.fastjson.JSON
import com.bjvca.dmp.commonutils.{ConfUtils, JedisPools}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD

object BgAdxStreamingReducing extends Logging {


  def countDealID(rdd: RDD[ConsumerRecord[String, String]], confUtils: ConfUtils) = {

    if (rdd.isEmpty()) {
      logWarning("empty rdd")
    }
    else {

      rdd.filter(line => {
        JSON.parseObject(line.value()).getString("sltcustomtopic").equals("sltadxlog-adx-bg")
      })
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
