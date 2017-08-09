package com.ming.java.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by Administrator on 2017-08-09.
 */
public class JDBCResource {
    public static void main(String args[]){
        SparkConf conf = new SparkConf()
                .setAppName("JDBCDataSource");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);

        // 总结一下
        // jdbc数据源
        // 首先，是通过SQLContext的read系列方法，将mysql中的数据加载为DataFrame
        // 然后可以将DataFrame转换为RDD，使用Spark Core提供的各种算子进行操作
        // 最后可以将得到的数据结果，通过foreach()算子，写入mysql、hbase、redis等等db / cache中
        Map<String,String> options=new HashMap<String, String>();
        options.put("url","jdbc:mysql://192.168.188.140:3306/test");
        options.put("dbtable","tb_user");
        options.put("user","root");
        options.put("password","123456");
        DataFrame tbUser = sqlContext.read().format("jdbc").options(options).load();
        tbUser.show();

    }
}
