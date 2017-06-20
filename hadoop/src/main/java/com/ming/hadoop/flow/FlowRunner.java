package com.ming.hadoop.flow;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Created by root on 6/18/17.
 */
public class FlowRunner extends Configured implements Tool{
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf=new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(FlowRunner.class);
        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowRedcer.class);
        job.setMapOutputValueClass(FlowBean.class);
        job.setMapOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);
        job.setOutputKeyClass(Text.class);
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        return job.waitForCompletion(true)?0:1;//打印运行进度
    }

    public static void main(String args[]) throws Exception {

        int run = ToolRunner.run(new Configuration(), new FlowRunner(), args);
        System.exit(run);

    }

}
