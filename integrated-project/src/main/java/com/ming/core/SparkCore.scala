package com.ming.core

import org.apache.spark.{SparkConf, SparkContext}

object SparkCore {
  def main(args: Array[String]): Unit = {
    val conf= new SparkConf().setAppName("Test").setMaster("local")
    val sc=new SparkContext(conf)
    val list=Array("hello","word","my","name","is")
    val rdd=sc.parallelize(list).map(x=>(x,1))
    rdd.reduceByKey(_+_).collect()
  }
}
