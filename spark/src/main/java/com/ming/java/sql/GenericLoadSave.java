package com.ming.java.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;

import java.util.List;

/**
 * Created by Administrator on 2017-08-09.
 */
public class GenericLoadSave  {
    public static void main(String args[]){
        SparkConf conf=new SparkConf();
        conf.setAppName("GenericLoadSave").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);
        DataFrame load = sqlContext.load("hdfs://ns1/spark-study/users.parquet");
        load.registerTempTable("users");
        DataFrame nameDF = sqlContext.sql("select name from users");
        List<String> nameList = nameDF.toJavaRDD().map(new Function<Row, String>() {
            public String call(Row v1) throws Exception {
                return "Name:" + v1.get(0);
            }
        }).collect();
        for (String name:nameList
             ) {
            System.out.print(name);
        }
    }
}
