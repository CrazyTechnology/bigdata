package com.ming.hadoop.mr;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by root on 6/16/17.
 * LongWritable 是hadoop 封装到long
 * Text  是 hadoop  封装到String
 *
 */
public class WordCountMapper extends Mapper<LongWritable,Text,Text,LongWritable> {
    /**
     * 每读一行数据执行一次方法
     * @param key 一行数据到开始偏移量
     * @param value 一行到内容
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] words = StringUtils.split(line, " ");

        for (String word:words
             ) {
                context.write(new Text(word),new LongWritable(1));
        }
    }
}
