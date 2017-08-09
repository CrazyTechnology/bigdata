package com.ming.scala

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.functions._
/**
  * Created by Administrator on 2017-08-09.
  */
object DailyUV {
  def main(args: Array[String]) {
    val sparpConf=new SparkConf().setAppName("DailySale").setMaster("local")
    val sparkContext=new SparkContext(sparpConf)
    val sqlContext=new SQLContext(sparkContext)
    import sqlContext.implicits._
    val userAccessLog = Array(
      "2015-10-01,1122",
      "2015-10-01,1122",
      "2015-10-01,1123",
      "2015-10-01,1124",
      "2015-10-01,1124",
      "2015-10-02,1122",
      "2015-10-02,1121",
      "2015-10-02,1123",
      "2015-10-02,1123");
    val userAccessLogRDD = sparkContext.parallelize(userAccessLog, 5)
    val userAccessLogRowRDD = userAccessLogRDD
      .map { log => Row(log.split(",")(0), log.split(",")(1).toInt) }
    // 构造DataFrame的元数据
    val structType = StructType(Array(
      StructField("date", StringType, true),
      StructField("userid", IntegerType, true)))
    // 使用SQLContext创建DataFrame
    val userAccessLogRowDF = sqlContext.createDataFrame(userAccessLogRowRDD, structType)
    userAccessLogRowDF.groupBy("date").agg(countDistinct('userid)).show()
  }
}
