package com.ming.java;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import java.util.Arrays;
import java.util.List;

/**
 * Created by root on 7/6/17.
 */
public class RDDTest {
    public static void main(String[] args){
        SparkConf conf=new SparkConf().setAppName("RDDTest").setMaster("local");
        //获取sparkcontext
        JavaSparkContext sc=new JavaSparkContext(conf);
        List<Integer> numbers = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        JavaRDD<Integer> numRDD = sc.parallelize(numbers);
        int sum= numRDD.reduce(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1+v2;
            }
        });
        System.out.println("1----50="+sum);

    }
}
