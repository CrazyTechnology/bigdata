package com.ming.java.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;

/**
 * Created by root on 7/3/17.
 */
public class WordCount {

    public static void main(String []args){

        //第一步设置spark配置信息,local代表本地运行
        SparkConf conf=new SparkConf();
        conf.setAppName("wordCountApp");
       // conf.setMaster("local");
        //第二步创建sparkcontext对象，java使用javasparkcontext对象
        JavaSparkContext  sparkContext=new JavaSparkContext(conf);
        //第三步，根据输入源（hdfs.hive）创建RDD
        // SparkContext中，用于根据文件类型的输入源创建RDD的方法，叫做textFile()方法
        // 在Java中，创建的普通RDD，都叫做JavaRDD
        // 在这里呢，RDD中，有元素这种概念，如果是hdfs或者本地文件呢，创建的RDD，每一个元素就相当于
        // 是文件里的一行
       // JavaRDD<String> lines = sparkContext.textFile("/hadoop/src/spark.txt");
        JavaRDD<String> lines = sparkContext.textFile("hdfs://ns1/spark.txt");
        //第四步，对初始RDD，进行transformation操作，也就是一些计算操作
        //通常使用创建function,并配合RDD的map,floatmap等算子来执行
        //function如果比较简单，通常使用创建匿名内部类，如果较复杂单独创建一个实现function接口的类
        // 先将每一行拆分成单个的单词
        // FlatMapFunction，有两个泛型参数，分别代表了输入和输出类型
        // 我们这里呢，输入肯定是String，因为是一行一行的文本，输出，其实也是String，因为是每一行的文本
        // 这里先简要介绍flatMap算子的作用，其实就是，将RDD的一个元素，给拆分成一个或多个元素
        JavaRDD<String> word = lines.flatMap(new FlatMapFunction<String, String>() {
            private static final long serialVersionUID = 1L;
            public Iterable<String> call(String line) throws Exception {
                return Arrays.asList(line.split(" "));
            }
        });

        // 接着，需要将每一个单词，映射为(单词, 1)的这种格式
        // 因为只有这样，后面才能根据单词作为key，来进行每个单词的出现次数的累加
        // mapToPair，其实就是将每个元素，映射为一个(v1,v2)这样的Tuple2类型的元素
        // 如果大家还记得scala里面讲的tuple，那么没错，这里的tuple2就是scala类型，包含了两个值
        // mapToPair这个算子，要求的是与PairFunction配合使用，第一个泛型参数代表了输入类型
        // 第二个和第三个泛型参数，代表的输出的Tuple2的第一个值和第二个值的类型
        // JavaPairRDD的两个泛型参数，分别代表了tuple元素的第一个值和第二个值的类型
        JavaPairRDD<String, Integer> mapToPair = word.mapToPair(
                new PairFunction<String, String, Integer>() {

                    private static final long serialVersionUID = 1L;

                    public Tuple2<String, Integer> call(String word) throws Exception {
                        return new Tuple2<String, Integer>(word, 1);
                    }
                });
        // 接着，需要以单词作为key，统计每个单词出现的次数
                    // 这里要使用reduceByKey这个算子，对每个key对应的value，都进行reduce操作
                    // 比如JavaPairRDD中有几个元素，分别为(hello, 1) (hello, 1) (hello, 1) (world, 1)
                    // reduce操作，相当于是把第一个值和第二个值进行计算，然后再将结果与第三个值进行计算
                    // 比如这里的hello，那么就相当于是，首先是1 + 1 = 2，然后再将2 + 1 = 3
                    // 最后返回的JavaPairRDD中的元素，也是tuple，但是第一个值就是每个key，第二个值就是key的value
                    // reduce之后的结果，相当于就是每个单词出现的次数
        JavaPairRDD<String, Integer> wordCounts = mapToPair.reduceByKey(

                new Function2<Integer, Integer, Integer>() {

                    private static final long serialVersionUID = 1L;

                    public Integer call(Integer v1, Integer v2) throws Exception {
                        return v1 + v2;
                    }

                });

        // 到这里为止，我们通过几个Spark算子操作，已经统计出了单词的次数
        // 但是，之前我们使用的flatMap、mapToPair、reduceByKey这种操作，都叫做transformation操作
        // 一个Spark应用中，光是有transformation操作，是不行的，是不会执行的，必须要有一种叫做action
        // 接着，最后，可以使用一种叫做action操作的，比如说，foreach，来触发程序的执行
        wordCounts.foreach(new VoidFunction<Tuple2<String,Integer>>() {

            private static final long serialVersionUID = 1L;

            public void call(Tuple2<String, Integer> wordCount) throws Exception {
                System.out.println(wordCount._1 + " appeared " + wordCount._2 + " times.");
            }

        });

        sparkContext.close();


    }

}
