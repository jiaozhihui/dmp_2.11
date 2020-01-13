package test

import org.apache.spark.sql.SparkSession

object CopyDataEsToLocal {

  def main(args: Array[String]): Unit = {

    val seq = Seq(
      (1, 2),
      (1, 2),
      (1, 2),
      (1, 2)
    )
    val sparkSql = SparkSession.builder()
      .appName(this.getClass.getSimpleName)
      .master("local")
      .getOrCreate()

    import sparkSql.implicits._
   seq.toDF("a","b-b").createOrReplaceTempView("aaa")

    sparkSql.sql(
      s"""
        |select
        |concat(`a`,`b-b`) as c
        |from aaa
        |""".stripMargin).show()


  }

}