package com.ming.scala.test

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by root on 7/4/17.
  */
object WordCount {

  def main(args: Array[String]): Unit = {
    val conf=new SparkConf();
    conf.setAppName("wordcount");
    val sc=new SparkContext(conf);
    val lines=sc.textFile("hdfs://ns1/spark.txt");
    val words=lines.flatMap(line =>line.split(" ") )
     val pairs=   words.map(word => (word,1))
      val wordCounts= pairs.reduceByKey(_ + _)
    wordCounts.foreach(wordcount => println(wordcount._1+"appears "+wordcount._2+" times"))

  }

}
