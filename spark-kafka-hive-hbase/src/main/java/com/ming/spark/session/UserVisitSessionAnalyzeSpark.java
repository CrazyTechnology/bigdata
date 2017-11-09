package com.ming.spark.session;

import com.ming.util.MockData;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.hive.HiveContext;

/**
 * Created by ming on 2017/10/11.
 */
public class UserVisitSessionAnalyzeSpark {
    public static void main(String args[]){
        SparkConf sparkConf=new SparkConf();
        sparkConf.setAppName("UserVisitSessionAnalyzeSpark")
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        SQLContext sqlContext=new HiveContext(sc.sc());
        // 生成模拟测试数据
        MockData.mock(sc, sqlContext);
        DataFrame sql = sqlContext.sql("select * from user_visit_action");
        sql.saveAsTable("user_vistit_action");


    }


}
