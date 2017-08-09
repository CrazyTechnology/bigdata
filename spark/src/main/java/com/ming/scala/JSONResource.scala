package com.ming.scala

import java.util.Date

import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types.{IntegerType, StructField, StringType, StructType}
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by Administrator on 2017-08-09.
  */
object JSONResource {

  def main(args: Array[String]) {
    val startTime=new Date();
    val conf = new SparkConf()
      .setAppName("JSONDataSource")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    // 创建学生成绩DataFrame
    val studentScoresDF = sqlContext.read.json("hdfs://ns1/spark-study/students.json")

    // 查询出分数大于80分的学生成绩信息，以及学生姓名
    studentScoresDF.registerTempTable("student_scores")
    val goodStudentScoresDF = sqlContext.sql("select name,score from student_scores where score>=80")
    val goodStudentNames = goodStudentScoresDF.rdd.map { row => row(0) }.collect()

    // 创建学生基本信息DataFrame
    val studentInfoJSONs = Array("{\"name\":\"Leo\", \"age\":18}",
      "{\"name\":\"Marry\", \"age\":17}",
      "{\"name\":\"Jack\", \"age\":19}")
    val studentInfoJSONsRDD = sc.parallelize(studentInfoJSONs, 3);
    val studentInfosDF = sqlContext.read.json(studentInfoJSONsRDD)

    // 查询分数大于80分的学生的基本信息
    studentInfosDF.registerTempTable("student_infos")

    var sql = "select name,age from student_infos where name in ("
    for(i <- 0 until goodStudentNames.length) {
      sql += "'" + goodStudentNames(i) + "'"
      if(i < goodStudentNames.length - 1) {
        sql += ","
      }
    }
    sql += ")"

    val goodStudentInfosDF = sqlContext.sql(sql)

    // 将分数大于80分的学生的成绩信息与基本信息进行join
    val goodStudentsRDD =
      goodStudentScoresDF.rdd.map { row => (row.getAs[String]("name"), row.getAs[Long]("score")) }
        .join(goodStudentInfosDF.rdd.map { row => (row.getAs[String]("name"), row.getAs[Long]("age")) })

    // 将rdd转换为dataframe
    val goodStudentRowsRDD = goodStudentsRDD.map(
      info => Row(info._1, info._2._1.toInt, info._2._2.toInt))

    val structType = StructType(Array(
      StructField("name", StringType, true),
      StructField("score", IntegerType, true),
      StructField("age", IntegerType, true)))
    val goodStudentsDF = sqlContext.createDataFrame(goodStudentRowsRDD, structType)
    goodStudentsDF.show()
    val endTime=new Date()
    val usetime=endTime.getTime-startTime.getTime
    System.out.println("运行时间：--------------------------------"+usetime)
  }

}
