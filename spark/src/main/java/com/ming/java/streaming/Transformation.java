package com.ming.java.streaming;

import com.google.common.base.Optional;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by ming on 2017/8/16.
 */
public class Transformation {
    public  static void main(String [] args){
        SparkConf sc=new SparkConf().setMaster("local[2]").setAppName("Update");
        //每隔1秒接受信息
        JavaStreamingContext jsc=new JavaStreamingContext(sc, Durations.seconds(5));
        JavaReceiverInputDStream<String> localhostDs = jsc.socketTextStream("localhost", 9999);
        jsc.checkpoint("hdfs://ns1/wordcount/");
        // 先做一份模拟的黑名单RDD
        List<Tuple2<String, Boolean>> blacklist = new ArrayList<Tuple2<String, Boolean>>();
        blacklist.add(new Tuple2<String, Boolean>("tom", true));
        final JavaPairRDD<String, Boolean> blacklistRDD = jsc.sc().parallelizePairs(blacklist);
        JavaPairDStream<String, String> mapToPair = localhostDs.mapToPair(new PairFunction<String, String, String>() {
            public Tuple2<String, String> call(String s) throws Exception {
                return new Tuple2<String, String>(s.split(" ")[1], s);
            }
        });
        JavaDStream<String> transform = mapToPair.transform(new Function<JavaPairRDD<String, String>, JavaRDD<String>>() {
            public JavaRDD<String> call(JavaPairRDD<String, String> stringStringJavaPairRDD) throws Exception {
                
                JavaPairRDD<String, Tuple2<String, Optional<Boolean>>> usefullInfo = stringStringJavaPairRDD.leftOuterJoin(blacklistRDD);

                JavaPairRDD<String, Tuple2<String, Optional<Boolean>>> filter = usefullInfo.filter(new Function<Tuple2<String, Tuple2<String, Optional<Boolean>>>, Boolean>() {
                    public Boolean call(Tuple2<String, Tuple2<String, Optional<Boolean>>> tuple) throws Exception {
                        if (tuple._2._2.isPresent() && tuple._2._2.get()) {
                            return false;
                        } else {
                            return true;
                        }
                    }
                });
                JavaRDD<String> map = filter.map(new Function<Tuple2<String, Tuple2<String, Optional<Boolean>>>, String>() {
                    public String call(Tuple2<String, Tuple2<String, Optional<Boolean>>> tuple) throws Exception {
                        return tuple._2._1;
                    }
                });

                return map;
            }
        });
        transform.print();
        jsc.start();
        jsc.awaitTermination();
        jsc.close();

    }
}
