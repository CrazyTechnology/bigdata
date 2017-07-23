package com.ming.bigdata.spark.session;
import com.alibaba.fastjson.JSONObject;
import com.ming.bigdata.conf.ConfigurationManager;
import com.ming.bigdata.constant.Constants;
import com.ming.bigdata.dao.DAOFactory;
import com.ming.bigdata.dao.ISessionAggrStatDAO;
import com.ming.bigdata.dao.ITaskDAO;
import com.ming.bigdata.domain.SessionAggrStat;
import com.ming.bigdata.domain.Task;
import com.ming.bigdata.util.*;
import org.apache.log4j.Logger;
import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;
import scala.Tuple2;

import java.util.Date;
import java.util.Iterator;

/**
 * Created by Administrator on 2017-07-21.
 * 用户行为分析
 */
public class UserSessionAnalyse {
    //记录日志
    private static Logger logger=Logger.getLogger(UserSessionAnalyse.class);
    public static void main(String [] args){
        SparkConf conf=new SparkConf().setAppName(Constants.SPARK_APP_NAME_SESSION).setMaster("local");
        JavaSparkContext sc=new JavaSparkContext(conf);
        SQLContext sql=getSqlContext(Constants.SPARK_LOCAL,sc);
        //生成模拟数据进行操作
        MockData.mock(sc,sql);
        // 创建需要使用的DAO组件
        ITaskDAO taskDAO = DAOFactory.getTaskDAO();

        // 首先得查询出来指定的任务，并获取任务的查询参数
        //long taskid = ParamUtils.getTaskIdFromArgs(args);
        long taskid = 2;
        Task task = taskDAO.findById(taskid);
        //taskParam 筛选条件
        JSONObject taskParam = JSONObject.parseObject(task.getTaskParam());
       //从临时表中读取数据并转换为RDD格式,user_visit_action
        JavaPairRDD<String, Row>  initUserVistActionPairRDD=createUservisitActionPairRdd(sql,taskParam);
        //以sessionId分组生成<sessionId,Iterator>格式的数据
        JavaPairRDD<String, Iterable<Row>> groupBySessionIdJavaPairRDD = initUserVistActionPairRDD.groupByKey();
        //获取user_vist_action和user_info表的具体信息，形成<sessionId,String>格式的rdd，获取sessionid的完整信息
        JavaPairRDD<String,String> fullSessionJavaPairRDD=createFullSessionJavaPairRdd(groupBySessionIdJavaPairRDD,sql);
        fullSessionJavaPairRDD.count();
        //根据指定条件进行筛选数据
        // 重构，同时进行过滤和统计
        Accumulator<String> sessionAggrStatAccumulator = sc.accumulator(
                "", new UserSessionAccumulator());

        JavaPairRDD<String,String> filterSessionByParamPairRdd=filterSessionByParam(fullSessionJavaPairRDD,taskParam,sessionAggrStatAccumulator);
        //根据筛选出来的数据进行分析
        // 计算出各个范围的session占比，并写入MySQL
        //accumulator.value()；获取的值是返回的自定义累加器add()方法的返回String值
        calculateAndPersistAggrStat(sessionAggrStatAccumulator.value(),task.getTaskid());


    }

    /**
     * 计算各个时长和步长在session总量中的占比，并写入mysql库中
     * @param value
     * @param taskid
     */
    private static void calculateAndPersistAggrStat(String value, long taskid) {
        // 从Accumulator统计串中获取值
        long session_count = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.SESSION_COUNT));

        long visit_length_1s_3s = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.TIME_PERIOD_1s_3s));
        long visit_length_4s_6s = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.TIME_PERIOD_4s_6s));
        long visit_length_7s_9s = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.TIME_PERIOD_7s_9s));
        long visit_length_10s_30s = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.TIME_PERIOD_10s_30s));
        long visit_length_30s_60s = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.TIME_PERIOD_30s_60s));
        long visit_length_1m_3m = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.TIME_PERIOD_1m_3m));
        long visit_length_3m_10m = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.TIME_PERIOD_3m_10m));
        long visit_length_10m_30m = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.TIME_PERIOD_10m_30m));
        long visit_length_30m = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.TIME_PERIOD_30m));

        long step_length_1_3 = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.STEP_PERIOD_1_3));
        long step_length_4_6 = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.STEP_PERIOD_4_6));
        long step_length_7_9 = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.STEP_PERIOD_7_9));
        long step_length_10_30 = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.STEP_PERIOD_10_30));
        long step_length_30_60 = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.STEP_PERIOD_30_60));
        long step_length_60 = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.STEP_PERIOD_60));

        // 计算各个访问时长和访问步长的范围
        double visit_length_1s_3s_ratio = NumberUtils.formatDouble(
                (double)visit_length_1s_3s / (double)session_count, 2);
        double visit_length_4s_6s_ratio = NumberUtils.formatDouble(
                (double)visit_length_4s_6s / (double)session_count, 2);
        double visit_length_7s_9s_ratio = NumberUtils.formatDouble(
                (double)visit_length_7s_9s / (double)session_count, 2);
        double visit_length_10s_30s_ratio = NumberUtils.formatDouble(
                (double)visit_length_10s_30s / (double)session_count, 2);
        double visit_length_30s_60s_ratio = NumberUtils.formatDouble(
                (double)visit_length_30s_60s / (double)session_count, 2);
        double visit_length_1m_3m_ratio = NumberUtils.formatDouble(
                (double)visit_length_1m_3m / (double)session_count, 2);
        double visit_length_3m_10m_ratio = NumberUtils.formatDouble(
                (double)visit_length_3m_10m / (double)session_count, 2);
        double visit_length_10m_30m_ratio = NumberUtils.formatDouble(
                (double)visit_length_10m_30m / (double)session_count, 2);
        double visit_length_30m_ratio = NumberUtils.formatDouble(
                (double)visit_length_30m / (double)session_count, 2);

        double step_length_1_3_ratio = NumberUtils.formatDouble(
                (double)step_length_1_3 / (double)session_count, 2);
        double step_length_4_6_ratio = NumberUtils.formatDouble(
                (double)step_length_4_6 / (double)session_count, 2);
        double step_length_7_9_ratio = NumberUtils.formatDouble(
                (double)step_length_7_9 / (double)session_count, 2);
        double step_length_10_30_ratio = NumberUtils.formatDouble(
                (double)step_length_10_30 / (double)session_count, 2);
        double step_length_30_60_ratio = NumberUtils.formatDouble(
                (double)step_length_30_60 / (double)session_count, 2);
        double step_length_60_ratio = NumberUtils.formatDouble(
                (double)step_length_60 / (double)session_count, 2);

        // 将统计结果封装为Domain对象
        SessionAggrStat sessionAggrStat = new SessionAggrStat();
        sessionAggrStat.setTaskid(taskid);
        sessionAggrStat.setSession_count(session_count);
        sessionAggrStat.setVisit_length_1s_3s_ratio(visit_length_1s_3s_ratio);
        sessionAggrStat.setVisit_length_4s_6s_ratio(visit_length_4s_6s_ratio);
        sessionAggrStat.setVisit_length_7s_9s_ratio(visit_length_7s_9s_ratio);
        sessionAggrStat.setVisit_length_10s_30s_ratio(visit_length_10s_30s_ratio);
        sessionAggrStat.setVisit_length_30s_60s_ratio(visit_length_30s_60s_ratio);
        sessionAggrStat.setVisit_length_1m_3m_ratio(visit_length_1m_3m_ratio);
        sessionAggrStat.setVisit_length_3m_10m_ratio(visit_length_3m_10m_ratio);
        sessionAggrStat.setVisit_length_10m_30m_ratio(visit_length_10m_30m_ratio);
        sessionAggrStat.setVisit_length_30m_ratio(visit_length_30m_ratio);
        sessionAggrStat.setStep_length_1_3_ratio(step_length_1_3_ratio);
        sessionAggrStat.setStep_length_4_6_ratio(step_length_4_6_ratio);
        sessionAggrStat.setStep_length_7_9_ratio(step_length_7_9_ratio);
        sessionAggrStat.setStep_length_10_30_ratio(step_length_10_30_ratio);
        sessionAggrStat.setStep_length_30_60_ratio(step_length_30_60_ratio);
        sessionAggrStat.setStep_length_60_ratio(step_length_60_ratio);

        // 调用对应的DAO插入统计结果
        ISessionAggrStatDAO sessionAggrStatDAO = DAOFactory.getSessionAggrStatDAO();
        sessionAggrStatDAO.insert(sessionAggrStat);
    }


    /**
     * 根据指定条件进行筛选
     * @param fullSessionJavaPairRDD
     * @return
     */
    private static JavaPairRDD<String,String> filterSessionByParam(JavaPairRDD<String, String> fullSessionJavaPairRDD, JSONObject taskParam, final Accumulator<String> accumulator) {
        // 为了使用我们后面的ValieUtils，所以，首先将所有的筛选参数拼接成一个连接串
        // 此外，这里其实大家不要觉得是多此一举
        // 其实我们是给后面的性能优化埋下了一个伏笔
        String startAge = ParamUtils.getParam(taskParam, Constants.PARAM_START_AGE);
        String endAge = ParamUtils.getParam(taskParam, Constants.PARAM_END_AGE);
        String professionals = ParamUtils.getParam(taskParam, Constants.PARAM_PROFESSIONALS);
        String cities = ParamUtils.getParam(taskParam, Constants.PARAM_CITIES);
        String sex = ParamUtils.getParam(taskParam, Constants.PARAM_SEX);
        String keywords = ParamUtils.getParam(taskParam, Constants.PARAM_KEYWORDS);
        String categoryIds = ParamUtils.getParam(taskParam, Constants.PARAM_CATEGORY_IDS);

        String _parameter = (startAge != null ? Constants.PARAM_START_AGE + "=" + startAge + "|" : "")
                + (endAge != null ? Constants.PARAM_END_AGE + "=" + endAge + "|" : "")
                + (professionals != null ? Constants.PARAM_PROFESSIONALS + "=" + professionals + "|" : "")
                + (cities != null ? Constants.PARAM_CITIES + "=" + cities + "|" : "")
                + (sex != null ? Constants.PARAM_SEX + "=" + sex + "|" : "")
                + (keywords != null ? Constants.PARAM_KEYWORDS + "=" + keywords + "|" : "")
                + (categoryIds != null ? Constants.PARAM_CATEGORY_IDS + "=" + categoryIds: "");

        if(_parameter.endsWith("\\|")) {
            _parameter = _parameter.substring(0, _parameter.length() - 1);
        }

        final String parameter = _parameter;
        //根据筛选条件过滤
        // 根据筛选参数进行过滤
        JavaPairRDD<String, String> filteredSessionid2AggrInfoRDD = fullSessionJavaPairRDD.filter(

                new Function<Tuple2<String,String>, Boolean>() {

                    private static final long serialVersionUID = 1L;

                    public Boolean call(Tuple2<String, String> tuple) throws Exception {
                        // 首先，从tuple中，获取聚合数据
                        String aggrInfo = tuple._2;

                        // 接着，依次按照筛选条件进行过滤
                        // 按照年龄范围进行过滤（startAge、endAge）
                        if(!ValidUtils.between(aggrInfo, Constants.FIELD_AGE,
                                parameter, Constants.PARAM_START_AGE, Constants.PARAM_END_AGE)) {
                            return false;
                        }

                        // 按照职业范围进行过滤（professionals）
                        // 互联网,IT,软件
                        // 互联网
                        if(!ValidUtils.in(aggrInfo, Constants.FIELD_PROFESSIONAL,
                                parameter, Constants.PARAM_PROFESSIONALS)) {
                            return false;
                        }

                        // 按照城市范围进行过滤（cities）
                        // 北京,上海,广州,深圳
                        // 成都
                        if(!ValidUtils.in(aggrInfo, Constants.FIELD_CITY,
                                parameter, Constants.PARAM_CITIES)) {
                            return false;
                        }

                        // 按照性别进行过滤
                        // 男/女
                        // 男，女
                        if(!ValidUtils.equal(aggrInfo, Constants.FIELD_SEX,
                                parameter, Constants.PARAM_SEX)) {
                            return false;
                        }

                        // 按照搜索词进行过滤
                        // 我们的session可能搜索了 火锅,蛋糕,烧烤
                        // 我们的筛选条件可能是 火锅,串串香,iphone手机
                        // 那么，in这个校验方法，主要判定session搜索的词中，有任何一个，与筛选条件中
                        // 任何一个搜索词相当，即通过
                        if(!ValidUtils.in(aggrInfo, Constants.FIELD_SEARCH_KEYWORDS,
                                parameter, Constants.PARAM_KEYWORDS)) {
                            return false;
                        }

                        // 按照点击品类id进行过滤
                        if(!ValidUtils.in(aggrInfo, Constants.FIELD_CLICK_CATEGORY_IDS,
                                parameter, Constants.PARAM_CATEGORY_IDS)) {
                            return false;
                        }

                        // 如果经过了之前的多个过滤条件之后，程序能够走到这里
                        // 那么就说明，该session是通过了用户指定的筛选条件的，也就是需要保留的session
                        // 那么就要对session的访问时长和访问步长，进行统计，根据session对应的范围
                        // 进行相应的累加计数

                        // 主要走到这一步，那么就是需要计数的session
                        accumulator.add(Constants.SESSION_COUNT);

                        // 计算出session的访问时长和访问步长的范围，并进行相应的累加
                        long visitLength = Long.valueOf(StringUtils.getFieldFromConcatString(
                                aggrInfo, "\\|", Constants.FIELD_VISIT_LENGTH));
                        long stepLength = Long.valueOf(StringUtils.getFieldFromConcatString(
                                aggrInfo, "\\|", Constants.FIELD_STEP_LENGTH));
                        calculateVisitLength(visitLength);
                        calculateStepLength(stepLength);

                        return true;
                    }

            /**
             * 计算访问步长
             * @param stepLength
             */
            private void calculateStepLength(long stepLength) {
                //根据访问步长，去匹配+1
                if(stepLength >= 1 && stepLength <= 3) {
                    accumulator.add(Constants.STEP_PERIOD_1_3);
                } else if(stepLength >= 4 && stepLength <= 6) {
                    accumulator.add(Constants.STEP_PERIOD_4_6);
                } else if(stepLength >= 7 && stepLength <= 9) {
                    accumulator.add(Constants.STEP_PERIOD_7_9);
                } else if(stepLength >= 10 && stepLength <= 30) {
                    accumulator.add(Constants.STEP_PERIOD_10_30);
                } else if(stepLength > 30 && stepLength <= 60) {
                    accumulator.add(Constants.STEP_PERIOD_30_60);
                } else if(stepLength > 60) {
                    accumulator.add(Constants.STEP_PERIOD_60);
                }
            }

            /**
             * 计算访问时长
             * @param visitLength
             */
            private void calculateVisitLength(long visitLength) {
                //根据访问时长，去匹配+1
                if(visitLength >=1 && visitLength <= 3) {
                    accumulator.add(Constants.TIME_PERIOD_1s_3s);
                } else if(visitLength >=4 && visitLength <= 6) {
                    accumulator.add(Constants.TIME_PERIOD_4s_6s);
                } else if(visitLength >=7 && visitLength <= 9) {
                    accumulator.add(Constants.TIME_PERIOD_7s_9s);
                } else if(visitLength >=10 && visitLength <= 30) {
                    accumulator.add(Constants.TIME_PERIOD_10s_30s);
                } else if(visitLength > 30 && visitLength <= 60) {
                    accumulator.add(Constants.TIME_PERIOD_30s_60s);
                } else if(visitLength > 60 && visitLength <= 180) {
                    accumulator.add(Constants.TIME_PERIOD_1m_3m);
                } else if(visitLength > 180 && visitLength <= 600) {
                    accumulator.add(Constants.TIME_PERIOD_3m_10m);
                } else if(visitLength > 600 && visitLength <= 1800) {
                    accumulator.add(Constants.TIME_PERIOD_10m_30m);
                } else if(visitLength > 1800) {
                    accumulator.add(Constants.TIME_PERIOD_30m);
                }
            }
        });
        filteredSessionid2AggrInfoRDD.foreach(new VoidFunction<Tuple2<String, String>>() {
            public void call(Tuple2<String, String> tuple) throws Exception {
                System.out.println("tuple1============================="+tuple._1);
                System.out.println("tuple2============================="+tuple._2);
            }
        });

         return filteredSessionid2AggrInfoRDD;

    }

    /**
     * 根据分组后的sessionid获取session的完整信息,计算出每个session的开始时间和结束时间差，也就是session会话的有效期，和session的步长
     * @param groupBySessionIdJavaPairRDD 根据sessionId分组后的pairRdd
     * @return
     */
    private static JavaPairRDD<String,String> createFullSessionJavaPairRdd(JavaPairRDD<String, Iterable<Row>> groupBySessionIdJavaPairRDD,SQLContext sqlcontext) {
        //获得一部分session的详细信息
        //将rdd转换为<userId,SessionInfo>格式
        JavaPairRDD<Long, String> partSessionInfoJavaPairRDD = groupBySessionIdJavaPairRDD.mapToPair(new PairFunction<Tuple2<String, Iterable<Row>>, Long, String>() {
            private static final long serialVersionUID = 1L;
            public Tuple2<Long, String> call(Tuple2<String, Iterable<Row>> tuple) throws Exception {
                Iterator<Row> iteratorSessionId = tuple._2.iterator();
                StringBuffer keyWordBuffer=new StringBuffer();
                StringBuffer clickCategoryIdBuffer=new StringBuffer();
                String sessionId=tuple._1;
                Long userId=null;
                // session的起始和结束时间
                Date startTime = null;
                Date endTime = null;
                // session的访问步长
                int stepLength = 0;
                while (iteratorSessionId.hasNext()) {
                    //遍历具有相同sessionid的访问信息
                    Row session = iteratorSessionId.next();
                    //获取userId
                    userId = session.getLong(1);
                    if(userId!=null){
                        userId=userId;
                    }
                    //关键字搜索，在一次session会话中可能会有多次key word搜索
                    String search_keyword=session.getString(5);
                    if(StringUtils.isNotEmpty(search_keyword)){
                        if(!keyWordBuffer.toString().contains(search_keyword)){
                            //判断如果之前的字符buffer中没有关键字就加入
                            keyWordBuffer.append(search_keyword+",");
                        }
                    }

                    //在一次session会话中点击的目录id
                    Long clickCategoryId=session.getLong(6);

                    if(clickCategoryId != null) {
                        if(!clickCategoryIdBuffer.toString().contains(
                                String.valueOf(clickCategoryId))) {
                            clickCategoryIdBuffer.append(clickCategoryId + ",");
                        }
                    }
                    //获取session访问时间action_time
                    // 计算session开始和结束时间
                    Date actionTime = DateUtils.parseTime(session.getString(4));
                    if(startTime==null){
                        startTime=actionTime;
                    }

                    if(endTime==null){
                        endTime=actionTime;
                    }
                    if(actionTime.before(startTime)){
                        startTime=actionTime;
                    }
                    if(actionTime.after(endTime)){
                        endTime=actionTime;
                    }
                    // 计算session访问步长
                    stepLength++;

                }
                String searchKeyWords = StringUtils.trimComma(keyWordBuffer.toString());
                String clickCategoryIds = StringUtils.trimComma(clickCategoryIdBuffer.toString());
                // 计算session访问时长（秒）
                long visitLength = (endTime.getTime() - startTime.getTime()) / 1000;


                String partAggrInfo = Constants.FIELD_SESSION_ID + "=" + sessionId + "|"
                        + Constants.FIELD_SEARCH_KEYWORDS + "=" + searchKeyWords + "|"
                        + Constants.FIELD_CLICK_CATEGORY_IDS + "=" + clickCategoryIds+"|"
                        + Constants.FIELD_VISIT_LENGTH + "=" + visitLength + "|"
                        + Constants.FIELD_STEP_LENGTH + "=" + stepLength;
                return new Tuple2<Long, String>(userId,partAggrInfo);
            }

        });

        //查询所有用的信息
        String sql="select * from user_info";
        JavaRDD<Row> userRdd = sqlcontext.sql(sql).javaRDD();
        JavaPairRDD<Long, Row> userInfoPariRdd = userRdd.mapToPair(new PairFunction<Row, Long, Row>() {
            public Tuple2<Long, Row> call(Row row) throws Exception {
                return new Tuple2<Long, Row>(row.getLong(0), row);
            }
        });

        //join操作，根据userId,查询所有的session用户信息
        //join的原理如下
        //a (1,spark)(2,hadoop)
        //b(1,30K)(2,40k)
        //a.join(b)
        //(1,(spark,30k))(2,(hadoop,40k))
        JavaPairRDD<Long, Tuple2<String, Row>> sessionAllUserInfoPariRdd = partSessionInfoJavaPairRDD.join(userInfoPariRdd);

        JavaPairRDD<String, String> sessionAllUserInfo = sessionAllUserInfoPariRdd.mapToPair(new PairFunction<Tuple2<Long, Tuple2<String, Row>>, String, String>() {
            private static final long serialVersionUID = 1L;

            public Tuple2<String, String> call(Tuple2<Long, Tuple2<String, Row>> tuple) throws Exception {
                //用户id
                Long userId = tuple._1();
                //获取到 部分session用户信息
                String partSessionUserInfo = tuple._2._1;
                //获取到 用户表中的每一行信息
                Row userInfoRow = tuple._2._2;

                //根据StringUtil获取值
                String sessionid = StringUtils.getFieldFromConcatString(
                        partSessionUserInfo, "\\|", Constants.FIELD_SESSION_ID);

                int age = userInfoRow.getInt(3);
                String professional = userInfoRow.getString(4);
                String city = userInfoRow.getString(5);
                String sex = userInfoRow.getString(6);
                String fulluserInfo = partSessionUserInfo + "|"
                        + Constants.FIELD_AGE + "=" + age + "|"
                        + Constants.FIELD_PROFESSIONAL + "=" + professional + "|"
                        + Constants.FIELD_CITY + "=" + city + "|"
                        + Constants.FIELD_SEX + "=" + sex;
                return new Tuple2<String, String>(sessionid, fulluserInfo);
            }
        });
            return sessionAllUserInfo;

    }

    /**
     * 从临时表中获取数据，并将rdd<row>根式转为<sessionId,row>格式
     * @param sqlcontext
     * @return <sessionId.row>格式的pairrdd
     */
    private static JavaPairRDD<String, Row> createUservisitActionPairRdd(SQLContext sqlcontext,JSONObject taskParam) {
        String startDate = ParamUtils.getParam(taskParam, Constants.PARAM_START_DATE);
        String endDate = ParamUtils.getParam(taskParam, Constants.PARAM_END_DATE);
        String sql =
                "select * "
                        + "from user_visit_action "
                        + "where date>='" + startDate + "' "
                        + "and date<='" + endDate + "'";
        //获取临时表中的数据
        DataFrame userVistActionDF = sqlcontext.sql(sql);
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
