package com.ming.scala

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by ming on 2017/8/16.
  */
object transformation {
  def main(args: Array[String]): Unit = {
    val sparkConf=new SparkConf().setMaster("local[2]").setAppName("transformation")
    val sparkStreaming=new StreamingContext(sparkConf,Seconds(5))
    val lines=sparkStreaming.socketTextStream("localhost",9999)
      sparkStreaming.checkpoint("hdfs://ns1/wordcount_dir")
    val blacklist = Array(("tom", true))
    val blacklistRDD = sparkStreaming.sparkContext.parallelize(blacklist, 5)
    val userAdsClickLogDStream = lines
      .map { adsClickLog => (adsClickLog.split(" ")(1), adsClickLog) }
    val validAdsClickLogDStream = userAdsClickLogDStream.transform(userAdsClickLogRDD => {
      val joinedRDD = userAdsClickLogRDD.leftOuterJoin(blacklistRDD)
      val filteredRDD = joinedRDD.filter(tuple => {
        if(tuple._2._2.getOrElse(false)) {
          false
        } else {
          true
        }
      })
      val validAdsClickLogRDD = filteredRDD.map(tuple => tuple._2._1)
      validAdsClickLogRDD
    })

    validAdsClickLogDStream.print()

    sparkStreaming.start()
    sparkStreaming.awaitTermination()


  }

}
