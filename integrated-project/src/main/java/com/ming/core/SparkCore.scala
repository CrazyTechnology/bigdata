package com.ming.core

import org.apache.spark.{SparkConf, SparkContext}

object SparkCore {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Test").setMaster("local")
    val sc = new SparkContext(conf)
    val list = Array("hello", "word", "my", "name", "is", "my")
    val rdd = sc.parallelize(list).map(x => (x, 1))
   val map= rdd.countByKey()
   val array= rdd.top(3)
   val value= map.get("my").getOrElse("")
    print(rdd.count())
  }
}
