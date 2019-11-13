package com.bjvca.dmp.adx.streaming

import org.apache.spark.rdd.RDD


object Reducing {

  val reduceRDD = (rdd: RDD[String]) => {

    rdd.foreachPartition(part=>{


      part.map(x=>)



    })



  }


}
