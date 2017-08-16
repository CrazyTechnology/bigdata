package com.ming.scala

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by Administrator on 2017-08-16.
  */
object Update {
  def main(args: Array[String]) {
    val sparkConf=new SparkConf().setMaster("local[2]").setAppName("update")
    val sparkStream=new StreamingContext(sparkConf,Seconds(1))
    sparkStream.checkpoint("hdfs://ns1/wordcount/")
    val lines=sparkStream.socketTextStream("localhost",9999)
         val wordcount= lines.flatMap(_.split(" ")).map(word=>(word,1))
      val wordCounts=wordcount.updateStateByKey((values:Seq[Int],state:Option[Int])=>{
        var newValue=state.getOrElse(0)
        for(value<-values){
          newValue+=value
        }
        Option(newValue)
      })
    wordCounts.print()
    sparkStream.start()
    sparkStream.awaitTermination()
    sparkStream.stop()
  }
}
