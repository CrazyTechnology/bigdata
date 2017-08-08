package com.ming.java.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

/**
 * Created by ming on 2017/8/8.
 */
public class ReflectTest {
    public static void main(String args[]){
        SparkConf conf=new SparkConf();
        conf.setAppName("DataFrameTest").setMaster("local");
        JavaSparkContext jc=new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(jc);
        JavaRDD<String> studentRDD = jc.textFile("/spark-study/students.txt");

        JavaRDD<Student> student = studentRDD.map(new Function<String, Student>() {
            public Student call(String str) throws Exception {
                String[] split = str.split(",");
                Student student = new Student();
                student.setAge(Integer.parseInt(split[2].trim()));
                student.setId(split[0]);
                student.setName(split[1]);
                return student;
            }
        });
        DataFrame studentFM = sqlContext.createDataFrame(student, Student.class);
        studentFM.show();
        studentFM.filter(studentFM.col("age").gt(18)).show();


    }
}
