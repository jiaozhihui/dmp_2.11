package test

import org.apache.spark.sql.SparkSession

object SparkSqlJson {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local")
      .appName(this.getClass.getSimpleName)
      .getOrCreate()

    val se = Seq {
      """{"material_name":"","adx_material_id":"1168317618873044992","dsp_material_id":"1168317618256613376","advertiser_id":"1155598184240779264","dsp_id":10001,"tag_id":"Scene_1023_OTT","tag_type":"","display_id":10001,"click_monitor":[],"pv_monitor":[],"info":{"image_src":"http://sltads-o.res.leiniao.com/adsslt/res/d0c59e62-1202-4112-bc79-dc169fb9152f.png","size":"380,200,i_size1","position":"left","effect":"effect1","out_url":"","md5":"1d5e6889dc9f8e56a84014a8c177ded7"}}"""
    }

    import spark.implicits._

    se.toDF("aaa").createOrReplaceTempView("abc")

    spark.sql(
      """
        |select
        |get_json_object(aaa,'$.dsp_material_id')
        |from abc
        |""".stripMargin).show()


  }

}
