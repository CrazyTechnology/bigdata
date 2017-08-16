package com.ming.scala

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2017-08-15.
  */
object WordCount {

  def main(args: Array[String]) {
    val sparkConf=new SparkConf().setMaster("local[2]").setAppName("WordCount")
    val sparkStreaming=new StreamingContext(sparkConf,Seconds(1))
    val lines=sparkStreaming.socketTextStream("localhost",9999)
    val words=lines.flatMap(_.split(" "))
    val pairs= words.map(word=>(word,1))
    val wordCount= pairs.reduceByKey(_+_)
    wordCount.print()
    Thread.sleep(5000)
    sparkStreaming.start()
    sparkStreaming.awaitTermination()

  }

}
