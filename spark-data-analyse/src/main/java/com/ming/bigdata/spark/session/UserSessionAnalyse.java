package com.ming.bigdata.spark.session;
import com.ming.bigdata.conf.ConfigurationManager;
import com.ming.bigdata.constant.Constants;
import com.ming.bigdata.util.MockData;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;

/**
 * Created by Administrator on 2017-07-21.
 * 用户行为分析
 */
public class UserSessionAnalyse {
    //记录日志
    private static Logger logger=Logger.getLogger(UserSessionAnalyse.class);
    public static void main(String args[]){
        SparkConf conf=new SparkConf().setAppName(Constants.SPARK_APP_NAME_SESSION).setMaster("local");
        JavaSparkContext sc=new JavaSparkContext(conf);
        SQLContext sql=getSqlContext(Constants.SPARK_LOCAL,sc);
        MockData.mock(sc,sql);
        DataFrame userInfo = sql.sql("select * from user_info");
        userInfo.insertIntoJDBC(ConfigurationManager.getProperty(Constants.JDBC_URL),"user_info",true);
        userInfo.show();
    }

    /**
     * 动态生成sqlcontext
     * @param sparkLocal
     * @param sc
     * @return
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
