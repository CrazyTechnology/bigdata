package com.ming.scala

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

/**
  * Created by ming on 2017/8/8.
  */
object DataFrameTest {

  def main(args: Array[String]): Unit = {
    val sparpConf=new SparkConf().setAppName("DataFrameTest").setMaster("local")
    val sparkContext=new SparkContext(sparpConf)
    val sqlContext=new SQLContext(sparkContext)
    val studentDF=sqlContext.jsonFile("hdfs://ns1/spark-study/students.json")
    studentDF.show()
    studentDF.select("name").show()
    studentDF.select(studentDF.col("age").plus(1),studentDF.col("name")).show()
    studentDF.filter("age>18").show()
    studentDF.groupBy("age").count().show()
  }

}
