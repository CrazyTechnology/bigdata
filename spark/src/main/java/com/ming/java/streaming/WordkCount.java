package com.ming.java.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 * Created by Administrator on 2017-08-15.
 */
public class WordkCount {
    public static void main(String args[]) throws InterruptedException {
        SparkConf sc=new SparkConf().setMaster("local[2]").setAppName("WordkCount");
        //每隔1秒接受信息
        JavaStreamingContext jsc=new JavaStreamingContext(sc, Durations.seconds(1));
        JavaReceiverInputDStream<String> localhostDs = jsc.socketTextStream("localhost", 9999);
        JavaDStream<String> wordString = localhostDs.flatMap(new FlatMapFunction<String, String>() {
            public Iterable<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" "));
            }
        });
        JavaPairDStream<String, Integer> stringIntegerJavaPairDStream = wordString.mapToPair(new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s, 1);
            }
        });
        JavaPairDStream<String, Integer> reduceByKey = stringIntegerJavaPairDStream.reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });
        reduceByKey.print();
        Thread.sleep(5000);
        //必须调用start方法，否则application不会执行
        jsc.start();
        jsc.awaitTermination();
        jsc.close();
    }
}
