package com.ming.scala

import org.apache.spark.sql.types.{StructField, StringType, StructType}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by Administrator on 2017-08-10.
  */
object UADF {
  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("UDAF")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    // 构造模拟数据
    val names = Array("Leo", "Marry", "Jack", "Tom", "Tom", "Tom", "Leo")
    val namesRDD = sc.parallelize(names, 5)
    val namesRowRDD = namesRDD.map { name => Row(name) }
    val structType = StructType(Array(StructField("name", StringType, true)))
    val namesDF = sqlContext.createDataFrame(namesRowRDD, structType)

    // 注册一张names表
    namesDF.registerTempTable("names")
    //注册自定义函数
    sqlContext.udf.register("strCount",new StringCount())
    //使用在定义函数
    sqlContext.sql("select name,strCount(name) from names group by name").collect().foreach(println)



  }

}
