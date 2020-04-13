package com.bjvca.videocut

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import com.bjvca.commonutils.ConfUtils
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.elasticsearch.spark._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.Random

object VideoCutMain extends Logging {

  def main(args: Array[String]): Unit = {

    logWarning("VideoCutMain开始运行")

//    val confUtil = new ConfUtils("application.conf")
        val confUtil = new ConfUtils("线上application.conf")

    // 创建sparkSession
    val spark = SparkSession.builder()
      .appName("VideoCutMain")
      .master("local")
      .getOrCreate()

    // 从mysql拿到数据，转化为json
    import spark.implicits._

    spark.read.format("jdbc")
      .options(Map(
        "url" -> s"jdbc:mysql://${confUtil.adseatMysqlHost}:3306/video_cut?characterEncoding=utf-8&useSSL=false",
        "driver" -> "com.mysql.jdbc.Driver",
        "user" -> confUtil.videocutMysqlUser,
        "password" -> confUtil.videocutMysqlPassword,
        "dbtable" -> "usable_media"
      )).load()
      .createOrReplaceTempView("bbb")

    spark.read.format("jdbc")
      .options(Map("url" -> s"jdbc:mysql://${confUtil.adseatMysqlHost}:3306/ssp_db?characterEncoding=utf-8&useSSL=false",
        "driver" -> "com.mysql.jdbc.Driver",
        "user" -> confUtil.adseatMysqlUser,
        "password" -> confUtil.adseatMysqlPassword,
        "dbtable" -> "ssp_ad_seat"
      ))
      .load()
      // 视频id、视频名、
      // 开始时间、结束时间
      // drama_name（剧集分类）, drama_type_name（剧集类型）
      // media_area_name（地区名）, media_release_data（上映年份）
      // 二级标签name
      // 一级标签id（分类用）、三级标签name
      .select($"video_id", $"media_name",
        $"ad_seat_b_time", $"ad_seat_e_time",
        $"drama_name", $"drama_type_name",
        $"media_area_name", $"media_release_date",
        $"class2_name",
        $"class_type_id", $"class3_name",
        $"ad_seat_img")
      .createOrReplaceTempView("aaa")

    spark.sql(
      """
        |select
        |aaa.video_id,
        |aaa.media_name,
        |aaa.ad_seat_b_time,
        |aaa.ad_seat_e_time,
        |aaa.drama_name,
        |aaa.drama_type_name,
        |aaa.media_area_name,
        |aaa.media_release_date,
        |aaa.class2_name,
        |aaa.class_type_id,
        |aaa.class3_name,
        |aaa.ad_seat_img
        |from aaa join bbb
        |on aaa.video_id=bbb.video_id
        |""".stripMargin)
      .createOrReplaceTempView("ccc")

    spark.sql("cache table ccc")

    val mysqlRDD = spark.sql("select * from ccc")
      .toJSON
      .rdd

    val reduced = mysqlRDD
      .filter(x => {
        val key = JSON.parseObject(x).get("class_type_id").toString
        key.equals("1") || key.equals("2") || key.equals("3") || key.equals("4")
      })
      // 处理数据为json格式，以video_id为key的元组
      .map(x => {
        val jsonArray = new JSONArray()
        val key = JSON.parseObject(x).get("video_id").toString
        jsonArray.add(x)
        (key, jsonArray)
      })
      // 将同一个video_id的reduce到一起，数据组成JSONArray
      .reduceByKey((x, y) => {
        for (i <- 0 until y.size()) {
          x.add(y.get(i))
        }
        x
      })

      /**
       * 预处理逻辑
       * 1、将所有的起止点，扩展3秒
       * 2、根据vid和class3Name分组，将一样的分到一个组
       * 3、按起点排序，得到所有标签的容器 adseatList
       * 4、遍历adseatList，创建一个空的标签位，和一个resultList
       * 5、如果adseattemp是null，则添加一个新的adseattemp，
       * 如果不为null，则判断两个能否合并，能合并则合并了等下一个标签，
       * 如果不能合并，则输出已合并的标签，然后将新标签设置为新的adseat
       * 6、返回resultList
       *
       */
      .map(x => {
        val vid = x._1
        val adseatJsonArray = x._2
        // 将时间点增大
        val adseatList = adseatJsonArray.toArray.map(adseatjson => {
          val nObject = JSON.parseObject(adseatjson.toString)
          val oldbtime = nObject.get("ad_seat_b_time").toString.toLong
          val oldetime = nObject.get("ad_seat_e_time").toString.toLong

          val newbtime = if (oldbtime.toLong - 5000 < 0) {
            0
          }
          else {
            (oldbtime.toLong - 5000)
          }

          val newetime = oldetime.toLong + 5000

          nObject.put("ad_seat_b_time", newbtime.toString)
          nObject.put("ad_seat_e_time", newetime.toString)

          nObject
        }).sortBy(y => y.get("ad_seat_b_time").toString.toLong)



        val resultList = new JSONArray()
//
        for (i<- 0 until adseatList.size){
          resultList.add(adseatList(i))
        }
        (vid,resultList)


//        var adseatTemp: JSONObject = null
//
//        for (i <- 0 until adseatList.size) {
//
//          if (adseatTemp == null) {
//            adseatTemp = adseatList(i).clone().asInstanceOf[JSONObject]
//          } else {
//
//
//            val tempETime = adseatTemp.get("ad_seat_e_time").toString.toLong
//            val adseatBTime = adseatList(i).get("ad_seat_b_time").toString.toLong
//
//            val adseatETime = adseatList(i).get("ad_seat_e_time").toString
//
//            if (adseatBTime - tempETime < 0) {
//              // 合并广告位，然后继续等待下一个标签
//
//              adseatTemp.put("ad_seat_e_time", adseatETime)
//
//            } else {
//              // 不合并，输出已有的广告位
//              resultList.add(adseatTemp.clone().asInstanceOf[JSONObject])
//              adseatTemp = null
//            }
//
//          }
//
//        }
//        (vid, resultList)
      })

      /**
       * 核心逻辑
       *
       * 对拿到的同一个video_id的一组视频进行处理
       * 将所有标签放到一个adseatMap中
       * 将所有起止点放到一个pointlist中
       * 1、创建一个空的tempMap
       * 2、遍历pointlist
       * 3、每拿到一个起始点，就从将对应的标签放到tempMap中
       * 4、输出resultMap中所有的视频片段到resultlist中
       * 5、每拿到一个终止点，就将对应的标签移除出tempMap
       * 6、重复3-4-5
       * 7、整理得到最终的resultMap
       */
      .map(x => {
        val vid = x._1
        val adseatJsonArray: JSONArray = x._2

        // 广告位的Map
        var adseatMap = mutable.Map[String, JSONObject]()
        // 起止点的List
        var pointList = ListBuffer[(String, JSONObject)]()
        // 缓存当前adseat的tempMap
        var tempMap = mutable.Map[String, JSONObject]()
        // 最终返回的数据resultList
        var resultList = ListBuffer[(String, JSONObject)]()


        // 遍历广告位JSON数组，将数据添加到adseatMap中
        // 遍历广告位数据，将所有起止点放到pointList中
        for (i <- 0 until adseatJsonArray.size()) {
          val jsonObject = JSON.parseObject(adseatJsonArray.get(i).toString)
          val class3Name = jsonObject.get("class3_name").toString
          val bTime = jsonObject.get("ad_seat_b_time").toString
          val eTime = jsonObject.get("ad_seat_e_time").toString

          // key
          val key = bTime + "-" + Random.nextInt(1000)
          adseatMap += (key -> jsonObject)

          // 起始点
          val bObject = new JSONObject
          bObject.put("point_type", "begin")
          bObject.put("adseat_key", key)
          pointList += ((bTime, bObject))
          // 终止点
          val eObject = new JSONObject
          eObject.put("point_type", "end")
          eObject.put("adseat_key", key)
          pointList += ((eTime, eObject))
        }

        val pointList2 = pointList.sortBy(_._1.toInt)

        var beginTime = ""
        var endTime = ""

        // 遍历所有point点，进而增加或减少tempMap中的adseat，进而处理处新片段
        for (i <- 0 until pointList2.size) {
          val (pointTime, thisPoint) = pointList2(i)
          val pointType = thisPoint.get("point_type").toString
          val adseatKey = thisPoint.get("adseat_key").toString

          //如果tempMap是空的，将pointTime赋值到开始时间
          if (tempMap.isEmpty) {
            // 初始化开始时间
            beginTime = pointTime
          } else {
            //设置本批次的结束时间为pointTime
            endTime = pointTime

            // 处理tempMap的数据，然后放到resultList中
            /**
             * 处理tempMap
             */
            // 先造出来一个片段的对象
            val tempJsonObj = new JSONObject()

            // 遍历tempMap，将这个片段内所包含的每个adseat数据处理进thisJsonObj
            val keys = tempMap.keys

            val manList = new JSONArray()
            val objectList = new JSONArray()
            val actionList = new JSONArray()
            val senceList = new JSONArray()

            val man2List = new JSONArray()
            val object2List = new JSONArray()
            val action2List = new JSONArray()
            val sence2List = new JSONArray()

            val manImgList = new JSONArray()
            val objectImgList = new JSONArray()
            val actionImgList = new JSONArray()
            val senceImgList = new JSONArray()

            for (key <- keys) {

              val thisObj: JSONObject = tempMap(key)
              val file1 = thisObj.get("video_id").asInstanceOf[String]
              val file2 = thisObj.get("media_name").asInstanceOf[String]
              val file3 = thisObj.get("drama_name").asInstanceOf[String]
              val file4 = thisObj.get("drama_type_name").asInstanceOf[String]
              val file5 = thisObj.get("media_area_name").asInstanceOf[String]
              val file6 = thisObj.get("media_release_date").toString
              val file7 = thisObj.get("class_type_id").toString
              val file8 = thisObj.get("class3_name").asInstanceOf[String]
              val file9 = thisObj.get("class2_name").asInstanceOf[String]
              val file10 = thisObj.get("ad_seat_img").asInstanceOf[String]

              tempJsonObj.put("string_vid", file1)
              tempJsonObj.put("media_name", file2)
              tempJsonObj.put("string_drama_name", file3)
              tempJsonObj.put("string_drama_type_name", file4)
              tempJsonObj.put("string_media_area_name", file5)
              tempJsonObj.put("string_media_release_date", file6)
              tempJsonObj.put("string_time", (beginTime + "_" + endTime))
              tempJsonObj.put("string_time_long", (endTime.toLong - beginTime.toLong).toString)

              file7 match {
                case "4" => {
                  manList.add(file8)
                  man2List.add(file9)
                  manImgList.add(file10)
                }
                case "1" => {
                  objectList.add(file8)
                  object2List.add(file9)
                  objectImgList.add(file10)
                }
                case "3" => {
                  actionList.add(file8)
                  action2List.add(file9)
                  actionImgList.add(file10)
                }
                case "2" => {
                  senceList.add(file8)
                  sence2List.add(file9)
                  senceImgList.add(file10)
                }
              }

            }

            tempJsonObj.put("string_man_list", manList)
            tempJsonObj.put("string_object_list", objectList)
            tempJsonObj.put("string_action_list", actionList)
            tempJsonObj.put("string_sence_list", senceList)

            tempJsonObj.put("string_man2_list", man2List)
            tempJsonObj.put("string_object2_list", object2List)
            tempJsonObj.put("string_action2_list", action2List)
            tempJsonObj.put("string_sence2_list", sence2List)

            tempJsonObj.put("string_man_img_list", manImgList)
            tempJsonObj.put("string_object_img_list", objectImgList)
            tempJsonObj.put("string_action_img_list", actionImgList)
            tempJsonObj.put("string_sence_img_list", senceImgList)

            resultList += ((adseatKey, tempJsonObj))

            // 设置本次结束的时间为下一批次的开始时间
            beginTime = endTime

          }

          // 处理完此point点前的片段后，然后针对此point点对tempMap操作
          if (pointType.equals("begin")) {
            // 如果是起始点，从adseatMap拿到对应数据，放到tempMap中
            tempMap += (adseatKey -> adseatMap(adseatKey))

          } else {
            // 如果是终止点，从tempMap中拿掉对应tempMap
            tempMap -= adseatKey

          }

        }

        // 最终返回resultList
        resultList
      })
      .flatMap(x => x.toArray[(String, JSONObject)])
      .map(x => {
        x._2.toString
      })
      .saveJsonToEs("videocut_cleaned/doc", Map(
        "es.index.auto.create" -> "true",
        "es.nodes" -> confUtil.adxStreamingEsHost,
        "es.port" -> "9200"
        //        "es.mapping.id" -> ""
      ))
  }

}