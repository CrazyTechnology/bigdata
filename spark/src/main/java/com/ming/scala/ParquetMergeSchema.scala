package com.ming.scala

import org.apache.hadoop.hive.ql.exec.spark.session.SparkSession
import org.apache.spark.sql.{SaveMode, SQLContext}
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by Administrator on 2017-08-09.
  */
object ParquetMergeSchema {
  def main(args: Array[String]) {
    val sparpConf=new SparkConf().setAppName("DataFrameTest").setMaster("local")
    val sparkContext=new SparkContext(sparpConf)
    val sqlContext=new SQLContext(sparkContext)
    import sqlContext.implicits._
    // 创建一个DataFrame，作为学生的基本信息，并写入一个parquet文件中
    val studentsWithNameAge = Array(("leo", 23), ("jack", 25)).toSeq
    val studentsWithNameAgeDF = sparkContext.parallelize(studentsWithNameAge, 2).toDF("name", "age")
    studentsWithNameAgeDF.save("/spark-study/students", "parquet", SaveMode.Append)

    // 创建第二个DataFrame，作为学生的成绩信息，并写入一个parquet文件中
    val studentsWithNameGrade = Array(("marry", "A"), ("tom", "B")).toSeq
    val studentsWithNameGradeDF = sparkContext.parallelize(studentsWithNameGrade, 2).toDF("name", "grade")
    studentsWithNameGradeDF.save("/spark-study/students", "parquet", SaveMode.Append)
    val students = sqlContext.read.option("mergeSchema", "true")
      .parquet("hdfs://ns1/spark-study/students")
    students.printSchema()
    students.show()
  }

}
