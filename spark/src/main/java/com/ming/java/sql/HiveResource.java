package com.ming.java.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.HiveContext;

/**
 * Created by Administrator on 2017-08-09.
 */
public class HiveResource {
    public static void main(String args[]){
        // 首先还是创建SparkConf
        SparkConf conf = new SparkConf()
                .setAppName("HiveDataSource");
        // 创建JavaSparkContext
        JavaSparkContext sc = new JavaSparkContext(conf);
        // 创建HiveContext，注意，这里，它接收的是SparkContext作为参数，不是JavaSparkContext
        HiveContext hiveContext = new HiveContext(sc.sc());
        // 第一个功能，使用HiveContext的sql()方法，可以执行Hive中能够执行的HiveQL语句
        hiveContext.sql("DROP TABLE IF EXISTS student_infos");
        hiveContext.sql("CREATE TABLE IF NOT EXISTS student_infos (name STRING, age INT)");
        hiveContext.sql("load data  local inpath '/hadoop/workjar/student_infos.txt'   into table student_infos");
        hiveContext.sql("DROP TABLE IF EXISTS student_scores");
        hiveContext.sql("CREATE TABLE IF NOT EXISTS student_scores (name STRING, score INT)");
        hiveContext.sql("load  data local inpath  '/hadoop/workjar/student_scores.txt' into table student_scores");
        DataFrame dataFrame = hiveContext.sql("select si.name, si.age, ss.score " +
                "FROM student_infos si " +
                "JOIN student_scores ss ON si.name=ss.name " +
                "WHERE ss.score>=80");
        hiveContext.sql("DROP TABLE IF EXISTS good_student_infos");
        dataFrame.saveAsTable("good_student_infos");
        Row[] good_student_infoses = hiveContext.table("good_student_infos").collect();
        for (Row row:good_student_infoses
             ) {
            System.out.println(row);
        }
        sc.close();
    }
}
