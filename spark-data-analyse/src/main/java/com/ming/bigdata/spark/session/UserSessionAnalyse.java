package com.ming.bigdata.spark.session;
import com.ming.bigdata.conf.ConfigurationManager;
import com.ming.bigdata.constant.Constants;
import com.ming.bigdata.util.MockData;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;
import scala.Tuple2;

import java.util.Iterator;

/**
 * Created by Administrator on 2017-07-21.
 * 用户行为分析
 */
public class UserSessionAnalyse {
    //记录日志
    private static Logger logger=Logger.getLogger(UserSessionAnalyse.class);
    public static void main(String args[]){
        SparkConf conf=new SparkConf().setAppName(Constants.SPARK_APP_NAME_SESSION);
        JavaSparkContext sc=new JavaSparkContext(conf);
        SQLContext sql=getSqlContext(Constants.SPARK_LOCAL,sc);
        //生成模拟数据进行操作
        MockData.mock(sc,sql);
       //从临时表中读取数据并转换为RDD格式,user_visit_action
        JavaPairRDD<String, Row>  initUserVistActionPairRDD=createUservisitActionPairRdd(sql);
        //以sessionId分组生成<sessionId,Iterator>格式的数据
        JavaPairRDD<String, Iterable<Row>> groupBySessionIdJavaPairRDD = initUserVistActionPairRDD.groupByKey();
        //获取user_vist_action和user_info表的具体信息，形成<sessionId,String>格式的rdd，获取sessionid的完整信息
        JavaPairRDD<String,String> fullSessionJavaPairRDD=createFullSessionJavaPairRdd(groupBySessionIdJavaPairRDD);


    }

    /**
     * 根据分组后的sessionid获取session的完整信息
     * @param groupBySessionIdJavaPairRDD 根据sessionId分组后的pairRdd
     * @return
     */
    private static JavaPairRDD<String,String> createFullSessionJavaPairRdd(JavaPairRDD<String, Iterable<Row>> groupBySessionIdJavaPairRDD) {
        //获得一部分session的详细信息
        JavaPairRDD<String, String> partSessionInfoJavaPairRDD = groupBySessionIdJavaPairRDD.mapToPair(new PairFunction<Tuple2<String, Iterable<Row>>, String, String>() {
            public Tuple2<String, String> call(Tuple2<String, Iterable<Row>> tuple) throws Exception {
                final long serialVersionUID = 1L;
                Iterator<Row> iteratorSessionId = tuple._2.iterator();
                while (iteratorSessionId.hasNext()) {
                    //遍历具有相同sessionid的访问信息
                    Row session = iteratorSessionId.next();
                    String date=session.get(0).toString();
                    String user_id=session.get(1).toString();
                }
                return new Tuple2<String, String>("1","1");
            }

        });
        return  partSessionInfoJavaPairRDD;
    }

    /**
     * 从临时表中获取数据，并将rdd<row>根式转为<sessionId,row>格式
     * @param sql
     * @return <sessionId.row>格式的pairrdd
     */
    private static JavaPairRDD<String, Row> createUservisitActionPairRdd(SQLContext sql) {
        //获取临时表中的数据
        DataFrame userVistActionDF = sql.sql("select * from user_visit_action");
        //将dataframe转为javaRDD
        JavaRDD<Row> userVistActionRDD = userVistActionDF.javaRDD();
        //将Rdd<row>格式转为Rdd<sessionId,row>格式
        JavaPairRDD<String, Row> userVistActionJavaPairRDD = userVistActionRDD.mapToPair(new PairFunction<Row, String, Row>() {
            public Tuple2<String, Row> call(Row row) throws Exception {
                  final long serialVersionUID = 1L;
                return new Tuple2<String, Row>(row.get(2).toString(), row);
            }
        });
        return userVistActionJavaPairRDD;
    }

    /**
     * 动态生成sqlcontext
     * @param sparkLocal 是否本地模式
     * @param sc javaSparkContext
     * @return sqlcontext
     */
    private static SQLContext getSqlContext(String sparkLocal,JavaSparkContext sc) {
        //判断是从hive中读取数据还是从本地生成数据
        if(ConfigurationManager.getBoolean(sparkLocal)){
            return new SQLContext(sc.sc());
        }else{
            return  new HiveContext(sc.sc());
        }
    }


}
