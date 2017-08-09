package com.ming.scala

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by Administrator on 2017-08-09.
  */
object GenericLoadSave {

  def main(args: Array[String]) {
    val sparkConf=new SparkConf()
    sparkConf.setAppName("GenericLoadSave").setMaster("local");
    val sparkContext=new SparkContext(sparkConf)
    val sqlContext=new SQLContext(sparkContext)
    val userParquet=sqlContext.load("hdfs://ns1/spark-study/gender=male/country=US/users.parquet")
    userParquet.show()
    userParquet.printSchema()
  }


}
