package com.ming.java.streaming;

import com.google.common.base.Optional;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * Created by Administrator on 2017-08-16.
 */
public class Update {
    public static void main(String args[]){
        SparkConf sc=new SparkConf().setMaster("local[2]").setAppName("Update");
        //每隔1秒接受信息
        JavaStreamingContext jsc=new JavaStreamingContext(sc, Durations.seconds(1));
        JavaReceiverInputDStream<String> localhostDs = jsc.socketTextStream("localhost", 9999);
        jsc.checkpoint("hdfs://ns1/wordcount/");
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
        JavaPairDStream<String, Integer> updateStateByKey = stringIntegerJavaPairDStream.updateStateByKey(new Function2<List<Integer>, Optional<Integer>, Optional<Integer>>() {
            public Optional<Integer> call(List<Integer> wordcount, Optional<Integer> v2) throws Exception {
                Integer newValue = 0;
                if (v2.isPresent()) {
                    newValue =v2.get();
                }
                for (Integer count : wordcount) {
                    newValue += count;
                }
                return Optional.of(newValue);
            }
        });
        updateStateByKey.print();
        jsc.start();
        jsc.awaitTermination();
        jsc.close();
    }
}
