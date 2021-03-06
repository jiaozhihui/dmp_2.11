package com.bjvca.dmp.adx.streaming

import java.util.Properties

import com.bjvca.commonutils.ConfUtils
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

object KafkaDataMaker {
  def main(args: Array[String]): Unit = {

    val confUtils = new ConfUtils("application.conf")

    val props = new Properties()
    props.put("bootstrap.servers", s"${confUtils.adxStreamingKafkaHost}:9092")
    props.put("acks", "all")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    var producer = new KafkaProducer[String, String](props)

    for (j<-0 until 971) {
      for (i <- 0 until 1) {

        val numbDeaiId = i + 1
        val record =
          s"""
             |{
             |"video_id":"01ce80bfe3abd07674ad8b95edbb16c1",
             |"adseat_id":"01ce80bfe3abd07674ad8b95edbb16c1",
             |"drama_id":"b",
             |"os":"",
             |"class_id":"",
             |"vis_id":"6599559023584903168",
             |"pkg_id":"",
             |"media_channel_id":"1023",
             |"act_id":"1622_9999",
             |"ad_id":"1622_9999",
             |"dsp_id":"10001",
             |"dealid":"1161124569789370368",
             |"order_id":"order_id00002",
             |"main_order_id":"order_id00002",
             |"advertiser_id":"zhangkaijiang",
             |"tag_id":"1023_scene_ott",
             |"adx_material_id":"1622_9999",
             |"sltcustomtopic":"sltadxlog-adx-bg"
             |}
           """.stripMargin

        val data = new ProducerRecord[String, String]("sltadxtopic", record)

        producer.send(data)

      }

      println("批次："+j)
      Thread.sleep(100)

    }

    producer.close()

  }

}