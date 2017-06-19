package com.ming.hadoop.flowbyarea;

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
 * Created by root on 6/19/17.
 * group by area
 */
public class AreaPartition {
    public static class FlowAreaMapper extends Mapper<LongWritable,Text,Text,FlowBean>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] values = StringUtils.split(line, "\t");
            String phoneNumber=values[1];
            long up_flow= Long.parseLong(values[7]);
            long down_flow= Long.parseLong(values[8]);
            context.write(new Text(phoneNumber),new FlowBean(phoneNumber,up_flow,down_flow));
        }
    }


    public static class FlowAreaReducer extends Reducer<Text,FlowBean,Text,FlowBean>{
        protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
            long up_flow_countter=0;
            long down_flow_countter=0;
            for (FlowBean flow:values
                 ) {
                up_flow_countter+=flow.getUp_flow();
                down_flow_countter+=flow.getD_flow();
            }

            context.write(key,new FlowBean(key.toString(),up_flow_countter,down_flow_countter));
        }
    }



    public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf=new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(AreaPartition.class);
        job.setMapperClass(FlowAreaMapper.class);
        job.setReducerClass(FlowAreaReducer.class);
        job.setPartitionerClass(Partintion.class);
        job.setMapOutputKeyClass(FlowBean.class);
        job.setMapOutputKeyClass(Text.class);
        job.setNumReduceTasks(9);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        System.exit(job.waitForCompletion(true)?0:1);

    }

}
