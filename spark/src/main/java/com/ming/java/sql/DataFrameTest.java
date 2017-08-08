package com.ming.java.sql;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructField;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by ming on 2017/8/8.
 */
public class DataFrameTest {

    public static void main(String args[]){
         SparkConf conf=new SparkConf();
        conf.setAppName("DataFrameTest").setMaster("local");
        JavaSparkContext  jc=new JavaSparkContext(conf);
        SQLContext sqlContext=new SQLContext(jc.sc());
        DataFrame studentDF = sqlContext.jsonFile("hdfs://ns1/spark-study/students.json");
        studentDF.show();
        //打印元数据信息
        studentDF.printSchema();
        //查询说有的name
        studentDF.select("name").show();
        studentDF.select(studentDF.col("age").plus(1),studentDF.col("name"),studentDF.col("id")).show();
        studentDF.filter("age>18").show();
        studentDF.groupBy(studentDF.col("age")).count().show();

    }

}
