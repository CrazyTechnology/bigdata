package com.ming.scala.rdd

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by root on 7/6/17.
  */
object RDDTest {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("ParallelizeCollection")
      .setMaster("local")
    val sc = new SparkContext(conf)
    val numbers = Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
    val numberRDD = sc.parallelize(numbers, 5)
    val sum = numberRDD.reduce(_ + _)
    println("1到10的累加和：" + sum)
  }

  def wordSum(): Unit ={
    val conf = new SparkConf()
      .setAppName("ParallelizeCollection")
      .setMaster("local")
    val sc = new SparkContext(conf)
    val numbers = Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
    val numberRDD = sc.parallelize(numbers, 5)
    val sum = numberRDD.reduce(_ + _)
    println("1到10的累加和：" + sum)
  }

}
