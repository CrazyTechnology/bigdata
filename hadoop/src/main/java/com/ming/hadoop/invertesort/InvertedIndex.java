package com.ming.hadoop.invertesort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Created by root on 6/20/17.
 */
public class InvertedIndex {
    public static class InvertMap extends Mapper<LongWritable,Text,Text,Text>{
        private Text keyInfo=new Text();
        private Text valueInfo=new Text();
        private FileSplit  split;
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            split= (FileSplit) context.getInputSplit();
            StringTokenizer tokenizer=new StringTokenizer(line," ");
            while (tokenizer.hasMoreTokens()){
                keyInfo.set(tokenizer.nextToken()+":"+split.getPath().getName());
                valueInfo.set("1");
                context.write(keyInfo,valueInfo);
            }

        }
    }

    public static class Combine extends Reducer<Text,Text,Text,Text>{
        private Text info=new Text();
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int splitIndex=key.toString().indexOf(":");
            int sum=0;
            for (Text value:values
                 ) {
                sum+=Integer.parseInt(value.toString());
            }
            info.set(key.toString().substring(splitIndex+1)+":"+sum);
            key.set(key.toString().substring(0,splitIndex));
            context.write(key,info);
        }
    }


    public static class  InvertReducer extends Reducer<Text,Text,Text,Text>{
        private Text result=new Text();
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String fileList=new String();
            for (Text value:values){
                fileList+=value.toString()+";";
            }
            result.set(fileList);
            context.write(key,result);
        }
    }


    public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf=new Configuration();
        Job job = Job.getInstance(conf);
            job.setJarByClass(InvertedIndex.class);
            job.setMapperClass(InvertMap.class);
            job.setCombinerClass(Combine.class);
            job.setReducerClass(InvertReducer.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            FileInputFormat.setInputPaths(job,new Path("/hadoop/tmp/"));
            FileOutputFormat.setOutputPath(job,new Path("/hadoop/tmp/output1"));
            System.exit(job.waitForCompletion(true)?0:1);
    }










}
