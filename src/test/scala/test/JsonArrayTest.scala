package test

import com.alibaba.fastjson.{JSONArray, JSONObject}

object JsonArrayTest {
  def main(args: Array[String]): Unit = {
    val nObject = new JSONObject()

    val array = new JSONArray()
    array.add("x")
    array.add("x")
    array.add("x")
    array.add("x")

    nObject.put("a",array)

    println(nObject)

  }

}
