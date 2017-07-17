package com.ming.java.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;

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
                "hdfs://ns1/spark-study/students.json");
        studentScoresDF.show();;
        studentScoresDF.printSchema();
        studentScoresDF.select("name");

        studentScoresDF.registerTempTable("student_score");
        //查询成绩大于80分的学生
        DataFrame goodeStudent = sqlContext.sql("select name,score  from student_score where score >80");
        goodeStudent.show();

        //加载学生信息
        DataFrame studentInfoDF = sqlContext.jsonFile(
                "hdfs://ns1/spark-study/people.json");
        studentInfoDF.registerTempTable("student_info");
        DataFrame infoAndScore = sqlContext.sql("select info.name,info.age,scores.score from student_info info left join student_score scores on info.name=scores.name");
        infoAndScore.save("hdfs://ns1/spark-study/goodstudentinfo.json", SaveMode.Append);
    }
}
