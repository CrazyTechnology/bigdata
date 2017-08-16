package com.ming.java.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by ming on 2017/8/15.
 */
public class KafkaWordCount {
    public static void main(String args[]){
        SparkConf conf = new SparkConf()
                .setMaster("local[2]")
                .setAppName("HDFSWordCount");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));
        Map<String,Integer> topicThreadMap=new HashMap<String, Integer>();
        topicThreadMap.put("wordcount",1);
        JavaPairReceiverInputDStream<String, String> lines = KafkaUtils.createStream(jssc, "192.168.190.128:2181,192.168.190.130:2181,192.168.190.131:2181", "", topicThreadMap);
        JavaDStream<String> words = lines.flatMap(new FlatMapFunction<Tuple2<String, String>, String>() {
            public Iterable<String> call(Tuple2<String, String> tuple) throws Exception {
                return Arrays.asList(tuple._2);
            }
        });
        JavaPairDStream<String, Integer> wordPair = words.mapToPair(new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s, 1);
            }
        });
        JavaPairDStream<String, Integer> reduceByKey = wordPair.reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });
        reduceByKey.print();
        jssc.start();
        jssc.awaitTermination();
    }
}
