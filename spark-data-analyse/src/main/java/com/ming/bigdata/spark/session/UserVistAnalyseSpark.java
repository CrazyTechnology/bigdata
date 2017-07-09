package com.ming.bigdata.spark.session;

import com.alibaba.fastjson.JSONObject;
import com.ming.bigdata.spark.constant.Constants;
import com.ming.bigdata.spark.dao.ITaskDAO;
import com.ming.bigdata.spark.dao.factory.DAOFactory;
import com.ming.bigdata.spark.domain.Task;
import com.ming.bigdata.spark.util.*;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;
import scala.Tuple2;

import java.util.Iterator;

/**
 * Created by root on 7/9/17.
 * * 用户访问session分析Spark作业
 *
 * 接收用户创建的分析任务，用户可能指定的条件如下：
 *
 * 1、时间范围：起始日期~结束日期
 * 2、性别：男或女
 * 3、年龄范围
 * 4、职业：多选
 * 5、城市：多选
 * 6、搜索词：多个搜索词，只要某个session中的任何一个action搜索过指定的关键词，那么session就符合条件
 * 7、点击品类：多个品类，只要某个session中的任何一个action点击过某个品类，那么session就符合条件
 *
 * 我们的spark作业如何接受用户创建的任务？
 *
 * J2EE平台在接收用户创建任务的请求之后，会将任务信息插入MySQL的task表中，任务参数以JSON格式封装在task_param
 * 字段中
 *
 * 接着J2EE平台会执行我们的spark-submit shell脚本，并将taskid作为参数传递给spark-submit shell脚本
 * spark-submit shell脚本，在执行时，是可以接收参数的，并且会将接收的参数，传递给Spark作业的main函数
 * 参数就封装在main函数的args数组中
 *
 * 这是spark本身提供的特性
 *
 */
public class UserVistAnalyseSpark {


    public static void main(String args[]){
        args = new String[]{"2"};
        /**
         * 初始化spark信息
         */
        SparkConf conf=new SparkConf();
        conf.setAppName(Constants.SPARK_APP_NAME_SESSION);
        conf.setMaster("local");
        JavaSparkContext jsc=new JavaSparkContext(conf);
        //获取sqlcontext
        SQLContext sqlContext=getSQLContext(jsc.sc());
        //生成模拟数据
        mockData(jsc,sqlContext);

        // 创建需要使用的DAO组件
        ITaskDAO taskDAO = DAOFactory.getTaskDAO();
        // 首先得查询出来指定的任务，并获取任务的查询参数
        long taskid = ParamUtils.getTaskIdFromArgs(args);
        Task task = taskDAO.findById(taskid);
        //获取查询条件
        JSONObject taskParam = JSONObject.parseObject(task.getTaskParam());
        // 如果要进行session粒度的数据聚合
        // 首先要从user_visit_action表中，查询出来指定日期范围内的行为数据
        JavaRDD<Row> DateRDD=getRDDInfoByDate(sqlContext,taskParam);
        System.out.println("==========="+ DateRDD.count());
        // 首先，可以将行为数据，按照session_id进行groupByKey分组
        // 此时的数据的粒度就是session粒度了，然后呢，可以将session粒度的数据
        // 与用户信息数据，进行join
        // 然后就可以获取到session粒度的数据，同时呢，数据里面还包含了session对应的user的信息
        JavaPairRDD<String, String> sessionid2AggrInfoRDD =
                aggregateBySession(sqlContext, DateRDD);

        System.out.print("+==========="+sessionid2AggrInfoRDD.count());

        // 接着，就要针对session粒度的聚合数据，按照使用者指定的筛选参数进行数据过滤
        // 相当于我们自己编写的算子，是要访问外面的任务参数对象的
        // 所以，大家记得我们之前说的，匿名内部类（算子函数），访问外部对象，是要给外部对象使用final修饰的
        JavaPairRDD<String, String> filteredSessionid2AggrInfoRDD =
                filterSession(sessionid2AggrInfoRDD, taskParam);
        System.out.print("+==========="+filteredSessionid2AggrInfoRDD.count());
        //关闭数据
        jsc.close();

    }

    /**
     * 过滤session数据
     * @param sessionid2AggrInfoRDD
     * @return
     */
    private static JavaPairRDD<String, String> filterSession(
            JavaPairRDD<String, String> sessionid2AggrInfoRDD,
            final JSONObject taskParam) {
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

        // 根据筛选参数进行过滤
        JavaPairRDD<String, String> filteredSessionid2AggrInfoRDD = sessionid2AggrInfoRDD.filter(

                new Function<Tuple2<String,String>, Boolean>() {

                    private static final long serialVersionUID = 1L;

                    @Override
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

                        return true;
                    }

                });

        return filteredSessionid2AggrInfoRDD;
    }

    /**
     * 对行为数据按session粒度进行聚合
     * @param dateRDD 行为数据RDD
     * @return session粒度聚合数据
     */
    private static JavaPairRDD<String,String> aggregateBySession(SQLContext sqlContext, JavaRDD<Row> dateRDD) {
        // 现在actionRDD中的元素是Row，一个Row就是一行用户访问行为记录，比如一次点击或者搜索
        // 我们现在需要将这个Row映射成<sessionid,Row>的格式
        JavaPairRDD<String, Row> javaPairRDD = dateRDD.mapToPair(new PairFunction<Row, String, Row>() {
            private static final long serialVersionUID = 1L;
            @Override
            public Tuple2<String, Row> call(Row row) throws Exception {
                return new Tuple2<String, Row>(row.getString(2),row);
            }
        });
        //根据sessionid分组
        JavaPairRDD<String, Iterable<Row>> pairRDDByGroup = javaPairRDD.groupByKey();

        // 对每一个session分组进行聚合，将session中所有的搜索词和点击品类都聚合起来
        // 到此为止，获取的数据格式，如下：<userid,partAggrInfo(sessionid,searchKeywords,clickCategoryIds)>
        JavaPairRDD<Long, String> longStringJavaPairRDD = pairRDDByGroup.mapToPair(new PairFunction<Tuple2<String, Iterable<Row>>, Long, String>() {
            private static final long serialVersionUID = 1L;

            @Override
            public Tuple2<Long, String> call(Tuple2<String, Iterable<Row>> tuple) throws Exception {
                //获取sessionid
                String sessionid = tuple._1();
                Iterator<Row> iterator = tuple._2().iterator();
                StringBuffer searchKeywordsBuffer = new StringBuffer("");
                StringBuffer clickCategoryIdsBuffer = new StringBuffer("");
                Long userid = null;
                while (iterator.hasNext()) {
                    // 提取每个访问行为的搜索词字段和点击品类字段
                    Row row = iterator.next();
                    if (userid == null) {
                        userid = row.getLong(1);
                    }
                    String searchKeyword = row.getString(5);
                    Long clickCategoryId = row.getLong(6);
                    // 实际上这里要对数据说明一下
                    // 并不是每一行访问行为都有searchKeyword何clickCategoryId两个字段的
                    // 其实，只有搜索行为，是有searchKeyword字段的
                    // 只有点击品类的行为，是有clickCategoryId字段的
                    // 所以，任何一行行为数据，都不可能两个字段都有，所以数据是可能出现null值的

                    // 我们决定是否将搜索词或点击品类id拼接到字符串中去
                    // 首先要满足：不能是null值
                    // 其次，之前的字符串中还没有搜索词或者点击品类id
                    if (StringUtils.isNotEmpty(searchKeyword)) {
                        if (!searchKeywordsBuffer.toString().contains(searchKeyword)) {
                            searchKeywordsBuffer.append(searchKeyword + ",");
                        }
                    }
                    if (clickCategoryId != null) {
                        if (!clickCategoryIdsBuffer.toString().contains(
                                String.valueOf(clickCategoryId))) {
                            clickCategoryIdsBuffer.append(clickCategoryId + ",");
                        }
                    }
                }
                String searchKeywords = StringUtils.trimComma(searchKeywordsBuffer.toString());
                String clickCategoryIds = StringUtils.trimComma(clickCategoryIdsBuffer.toString());
                // 大家思考一下
                // 我们返回的数据格式，即使<sessionid,partAggrInfo>
                // 但是，这一步聚合完了以后，其实，我们是还需要将每一行数据，跟对应的用户信息进行聚合
                // 问题就来了，如果是跟用户信息进行聚合的话，那么key，就不应该是sessionid
                // 就应该是userid，才能够跟<userid,Row>格式的用户信息进行聚合
                // 如果我们这里直接返回<sessionid,partAggrInfo>，还得再做一次mapToPair算子
                // 将RDD映射成<userid,partAggrInfo>的格式，那么就多此一举

                // 所以，我们这里其实可以直接，返回的数据格式，就是<userid,partAggrInfo>
                // 然后跟用户信息join的时候，将partAggrInfo关联上userInfo
                // 然后再直接将返回的Tuple的key设置成sessionid
                // 最后的数据格式，还是<sessionid,fullAggrInfo>

                // 聚合数据，用什么样的格式进行拼接？
                // 我们这里统一定义，使用key=value|key=value
                String partAggrInfo = Constants.FIELD_SESSION_ID + "=" + sessionid + "|"
                        + Constants.FIELD_SEARCH_KEYWORDS + "=" + searchKeywords + "|"
                        + Constants.FIELD_CLICK_CATEGORY_IDS + "=" + clickCategoryIds;

                return new Tuple2<Long, String>(userid, partAggrInfo);
            }
        });

        // 查询所有用户数据，并映射成<userid,Row>的格式
        String sql = "select * from user_info";
        JavaRDD<Row> userInfoRDD = sqlContext.sql(sql).javaRDD();
        JavaPairRDD<Long, Row> userid2InfoRDD = userInfoRDD.mapToPair(

                new PairFunction<Row, Long, Row>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Tuple2<Long, Row> call(Row row) throws Exception {
                        return new Tuple2<Long, Row>(row.getLong(0), row);
                    }

                });
        // 将session粒度聚合数据，与用户信息进行join
        JavaPairRDD<Long, Tuple2<String, Row>> userid2FullInfoRDD =
                longStringJavaPairRDD.join(userid2InfoRDD);


        // 对join起来的数据进行拼接，并且返回<sessionid,fullAggrInfo>格式的数据
        JavaPairRDD<String, String> sessionid2FullAggrInfoRDD = userid2FullInfoRDD.mapToPair(

                new PairFunction<Tuple2<Long,Tuple2<String,Row>>, String, String>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Tuple2<String, String> call(
                            Tuple2<Long, Tuple2<String, Row>> tuple)
                            throws Exception {
                        String partAggrInfo = tuple._2._1;
                        Row userInfoRow = tuple._2._2;

                        String sessionid = StringUtils.getFieldFromConcatString(
                                partAggrInfo, "\\|", Constants.FIELD_SESSION_ID);

                        int age = userInfoRow.getInt(3);
                        String professional = userInfoRow.getString(4);
                        String city = userInfoRow.getString(5);
                        String sex = userInfoRow.getString(6);

                        String fullAggrInfo = partAggrInfo + "|"
                                + Constants.FIELD_AGE + "=" + age + "|"
                                + Constants.FIELD_PROFESSIONAL + "=" + professional + "|"
                                + Constants.FIELD_CITY + "=" + city + "|"
                                + Constants.FIELD_SEX + "=" + sex;

                        return new Tuple2<String, String>(sessionid, fullAggrInfo);
                    }

                });

        return sessionid2FullAggrInfoRDD;


    }

    /**
     * 查询出来指定日期范围内的行为数据
     * @param sqlContext
     * @param taskParam 任务参数
     * @return
     */
    private static JavaRDD<Row> getRDDInfoByDate(SQLContext sqlContext, JSONObject taskParam) {
        String startDate = ParamUtils.getParam(taskParam, Constants.PARAM_START_DATE);
        String endDate = ParamUtils.getParam(taskParam, Constants.PARAM_END_DATE);
        String sql=
                "select * "
                        + "from user_visit_action "
                        + "where date>='" + startDate + "' "
                        + "and date<='" + endDate + "'";
        DataFrame dataFrame = sqlContext.sql(sql);
         return dataFrame.javaRDD();

    }


    /**
     *根据sparkConf创建sql上下文
     * @return  返回sql上下文
     */
    private static SQLContext getSQLContext(SparkContext sc) {
        boolean local = ConfigureUtil.getBoolean(Constants.SPARK_LOCAL.toString());
        if(local) {
            return new SQLContext(sc);
        } else {
            return new HiveContext(sc);
        }
    }

    /**
     * 生成模拟数据（只有本地模式，才会去生成模拟数据）
     * @param sc
     * @param sqlContext
     */
    private static void mockData(JavaSparkContext sc, SQLContext sqlContext) {
        boolean local = ConfigureUtil.getBoolean(Constants.SPARK_LOCAL.toString());
        if(local) {
            MockData.mock(sc, sqlContext);
        }
    }

}
