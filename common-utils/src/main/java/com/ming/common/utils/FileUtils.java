package com.ming.common.utils;

import java.io.File;
import java.io.FileOutputStream;
import java.sql.Connection;

public class FileUtils {

    public static void main(String []args){
        Connection connection = JDBCUtils.getConnection();
        String inputPath="";
        String dimensions="\"latitude\",\"longitude\",\"area\",\"city\",\"province\",\"country\",\"network\",\"ifrobot\",\"url\",\"activity\",\"referer\",\"resolution\",\"os\",\"os_version\",\"browser\",\"browser_version\",\"app_version\",\"session_source\",\"campaign_source\",\"campaign_medium\",\"campaign_name\",\"campaign_term\",\"campaign_content\",\"event_id\",\"event_name\",\"product_name\",\"sku_id\",\"spu_name\",\"sku_name\",\"category_1\",\"category_2\",\"category_3\",\"payment_method\",\"receiver_province\",\"receiver_city\",\"receiver_area\",\"discount_name\",\"discount_id\",\"discount_category\",\"auto_cancel_order\"";
        String metricsSpec="{\"type\": \"count\",\"name\": \"count\"},{\"name\": \"user_id\",\"type\": \"hyperUnique\",\"fieldName\": \"user_id\",\"round\":true},{\"name\": \"device_id\",\"type\": \"hyperUnique\",\"fieldName\": \"device_id\",\"round\":true },{\"name\": \"imei\",\"type\": \"hyperUnique\",\"fieldName\": \"imei\",\"round\":true},{\"name\": \"visitor_id\",\"type\": \"hyperUnique\",\"fieldName\": \"visitor_id\",\"round\":true},{\"name\": \"android_id\",\"type\": \"hyperUnique\",\"fieldName\": \"android_id\",\"round\":true},{\"name\": \"refund_id\",\"type\": \"hyperUnique\",\"fieldName\": \"refund_id\",\"round\":true},{\"name\": \"session_id\",\"type\": \"hyperUnique\",\"fieldName\": \"session_id\",\"round\":true},{\"name\": \"order_id\",\"type\": \"hyperUnique\",\"fieldName\": \"order_id\",\"round\":true},{\"name\": \"discount_amount\",\"type\": \"doubleSum\",\"fieldName\": \"discount_amount\"},{\"name\": \"goods_amount\",\"type\": \"doubleSum\",\"fieldName\": \"goods_amount\"},{\"name\": \"order_real_amount\",\"type\": \"doubleSum\",\"fieldName\": \"order_real_amount\"},{\"name\": \"goods_total_amount\",\"type\": \"doubleSum\",\"fieldName\": \"goods_total_amount\"},{\"name\": \"goods_real_amount\",\"type\": \"doubleSum\",\"fieldName\": \"goods_real_amount\"},{\"name\": \"real_refund\",\"type\": \"doubleSum\",\"fieldName\": \"real_refund\"},{\"name\": \"goods_nums\",\"type\": \"longSum\",\"fieldName\": \"goods_nums\"}";
        String sourceString = "{\n" +
                "        \"type\": \"index_hadoop\",\n" +
                "        \"spec\": {\n" +
                "                \"ioConfig\": {\n" +
                "                        \"type\": \"hadoop\",\n" +
                "                        \"inputSpec\": {\n" +
                "                                \"type\": \"granularity\",\n" +
                "                                \"dataGranularity\": \"day\",\n" +
                "                                \"inputPath\": \"hdfs://data-platform-production/apps/spark/utc/\",\n" +
                "                                \"filePattern\": \".*json|.*txt\",\n" +
                "                                \"pathFormat\": \'y'=yyyy/'m'=MM/'d'=dd/\"\n" +
                "                        }\n" +
                "                },\n" +
                "                \"dataSchema\": {\n" +
                "                        \"dataSource\": \"mall_bi\",\n" +
                "                        \"granularitySpec\": {\n" +
                "                                \"type\": \"uniform\",\n" +
                "                                \"segmentGranularity\": \"day\",\n" +
                "                                \"queryGranularity\": \"none\",\n" +
                "                                \"intervals\": [\"2018-07-09T00:00:00Z/2018-07-10T00:00:00Z\"]\n" +
                "                        },\n" +
                "                        \"parser\": {\n" +
                "                                \"type\": \"hadoopyString\",\n" +
                "                                \"parseSpec\": {\n" +
                "                                        \"format\": \"json\",\n" +
                "                                        \"dimensionsSpec\": {\n" +
                "                                                \"spatialDimensions\": [{\n" +
                "                                                        \"dimName\": \"geo\",\n" +
                "                                                        \"dims\": [\"latitude\", \"longitude\"]\n" +
                "                                                }],\n" +
                "                                                \"dimensions\": ["+dimensions+
                "                                                ]\n" +
                "                                        },\n" +
                "                                        \"timestampSpec\": {\n" +
                "                                                \"format\": \"millis\",\n" +
                "                                                \"column\": \"ts\"\n" +
                "                                        }\n" +
                "                                }\n" +
                "                        },\n" +
                "                        \"metricsSpec\": [\n" +metricsSpec+
                "                        ]\n" +
                "                },\n" +
                "                \"tuningConfig\": {\n" +
                "                        \"type\": \"hadoop\",\n" +
                "                        \"partitionsSpec\": {\n" +
                "                                \"type\": \"hashed\",\n" +
                "                                \"targetPartitionSize\": 5000000\n" +
                "                        },\n" +
                "                        \"jobProperties\": {\n" +
                "                                \"mapreduce.jobtracker.staging.root.dir\":\"/tmp/druid/hadoop\",\n" +
                "                                 \"yarn.app.mapreduce.am.staging-dir\":\"/tmp/druid/hadoop/hadoop-yarn/staging\",\n" +
                "                                 \"mapreduce.map.memory.mb\":\"10240\",\n" +
                "                                 \"mapreduce.reduce.memory.mb\":\"10240\",\n" +
                "\t\t\t\"mapreduce.job.classloader\":\"true\"\n" +
                "                                        }\n" +
                "                },\n" +
                "                \"hadoopDependencyCoordinates\": [\n" +
                "                                 \"org.apache.hadoop:hadoop-client:2.7.3\"\n" +
                "                ]\n" +
                "        }\n" +
                "}";	//待写入字符串
        byte[] sourceByte = sourceString.getBytes();
        if(null != sourceByte){
            try {
                File file = new File("/Users/limingming/test");		//文件路径（路径+文件名）
                if (!file.exists()) {	//文件不存在则创建文件，先创建目录
                    File dir = new File(file.getParent());
                    dir.mkdirs();
                    file.createNewFile();
                }
                FileOutputStream outStream = new FileOutputStream(file);	//文件输出流用于将数据写入文件
                outStream.write(sourceByte);
                outStream.close();	//关闭文件输出流
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
