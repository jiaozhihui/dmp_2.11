package test

import com.alibaba.fastjson.{JSONArray, JSONObject}

object JsonArrayTest {
  def main(args: Array[String]): Unit = {

    val list = scala.collection.mutable.ListBuffer[JSONObject]()

    var aaa =new  JSONObject

    aaa.put("1","1")

    list += aaa

    println(list.toString())

    aaa = null

    println(list.toString())
    list += aaa
    println(list.toString())





  }

}
