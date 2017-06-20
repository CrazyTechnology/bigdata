package com.ming.hadoop.flow;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by root on 6/18/17.
 */
public class FlowMapper extends Mapper<LongWritable,Text,Text,FlowBean> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();  //获得一行的数据

        String[] values = StringUtils.split(line, "\t");
        //拿到需要的字段
        String phoneNumber=values[1];
        long   up_flow= Long.parseLong(values[7]);
        long   down_flow= Long.parseLong(values[8]);

        //封装成kv形式
        context.write(new Text(phoneNumber),new FlowBean(phoneNumber,up_flow,down_flow));

    }
}
