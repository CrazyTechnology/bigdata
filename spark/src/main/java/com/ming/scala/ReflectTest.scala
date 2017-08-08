package com.ming.scala

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

/**
  * Created by ming on 2017/8/8.
  */
object ReflectTest extends App{
  val sparpConf=new SparkConf().setAppName("DataFrameTest").setMaster("local")
  val sparkContext=new SparkContext(sparpConf)
  val sqlContext=new SQLContext(sparkContext)
  import  sqlContext.implicits._
  case class Student(id:String,name:String,age:Int)
  val studentRDD=sparkContext.textFile("/spark-study/students.txt")
  val studentDf=studentRDD.map(line=>line.split(",")).map(arry=>Student(arry(0).toString,arry(1).toString,arry(2).toInt)).toDF()
  studentDf.registerTempTable("student");
  sqlContext.sql("select * from student").show();

}
