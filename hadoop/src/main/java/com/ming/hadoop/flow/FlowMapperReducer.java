package com.ming.hadoop.flow;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by root on 6/18/17.
 */
public class FlowMapperReducer {

    public static class FlowMapper extends Mapper<LongWritable,Text,FlowBean,NullWritable>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] values = StringUtils.split(line, "\t");
            String phoneNumber=values[0];
            long up_flow= Long.parseLong(values[1]);
            long down_flow= Long.parseLong(values[2]);
            context.write(new FlowBean(phoneNumber,up_flow,down_flow),NullWritable.get());
        }
    }



    public static class FlowReducer extends Reducer<FlowBean,NullWritable,FlowBean,NullWritable>{
        protected void reduce(FlowBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            context.write(key,NullWritable.get());
        }
    }


    public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf=new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(FlowMapperReducer.class);
        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReducer.class);
        job.setMapOutputValueClass(FlowBean.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(FlowBean.class);
        job.setOutputKeyClass(NullWritable.class);
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        System.exit(job.waitForCompletion(true)?0:1);
    }

}
