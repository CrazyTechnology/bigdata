package com.ming.java.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
/**
 * Created by Administrator on 2017-07-17.
 */
public class JsonSqlTest {

    public static void main(String args[]){
        SparkConf conf = new SparkConf()
                .setAppName("JsonSqlTest");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);
        // 针对json文件，创建DataFrame（针对json文件创建DataFrame）
        DataFrame studentScoresDF = sqlContext.jsonFile(
                "hdfs://nns:9000/spark-study/students.json");

    }
}
