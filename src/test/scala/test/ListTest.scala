package test

import com.alibaba.fastjson.JSONObject

import scala.collection.mutable.ListBuffer

object ListTest {
  def main(args: Array[String]): Unit = {
    var list = ListBuffer[(Int,JSONObject)]()

    val bObject = new JSONObject
    bObject.put("point_type", "begin")
    bObject.put("adseat_key", "1")
    val aaa=list :+ ((1,bObject))
    list += ((3,bObject))
    list += ((2,bObject))
    list -= ((2,bObject))
    list -= ((3,bObject))

    list.foreach(x=>println("aaa"+x))
    aaa.foreach(x=>println("aaa"+x))

    println(list.toString())
  }

}
