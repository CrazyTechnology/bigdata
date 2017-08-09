package com.ming.scala

import org.apache.spark.sql.types.{StructType, StringType, StructField, DoubleType}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.functions._
/**
  * Created by Administrator on 2017-08-09.
  */
object DailySale {

  def main(args: Array[String]) {
    val sparpConf=new SparkConf().setAppName("DailySale").setMaster("local")
    val sparkContext=new SparkContext(sparpConf)
    val sqlContext=new SQLContext(sparkContext)
    // 模拟数据
    val userSaleLog = Array("2015-10-01,55.05,1122",
      "2015-10-01,23.15,1133",
      "2015-10-01,15.20,",
      "2015-10-02,56.05,1144",
      "2015-10-02,78.87,1155",
      "2015-10-02,113.02,1123")
    val userSaleLogRDD = sparkContext.parallelize(userSaleLog, 5)
    //日志里面可能丢失了信息，过滤掉信息不全的数据
   val filterUser= userSaleLogRDD.filter(log=>
        if(log.split(",").length==3)
          true
        else
          false).coalesce(3)

   val info= filterUser.map(log=>Row(log.split(",")(0),log.split(",")(1).toDouble))
    val structType = StructType(Array(
      StructField("date", StringType, true),
      StructField("sale_amount", DoubleType, true)))
    val userSaleLogDF = sqlContext.createDataFrame(info, structType)
    import sqlContext.implicits._
    userSaleLogDF.groupBy("date").agg('date, sum('sale_amount)).map { row => Row(row(1), row(2)) }
      .collect()
      .foreach(println)
  }

}
