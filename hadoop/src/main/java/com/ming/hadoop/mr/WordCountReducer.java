package com.ming.hadoop.mr;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by root on 6/16/17.
 */
public class WordCountReducer extends Reducer<Text,LongWritable,Text,LongWritable> {

    /**
     * 框架在map处理完成之后，将所有kv对分组，然后传递一组<key,value{}> ,调用一次reduce方法
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        long count=0;
        //遍历values的list
        for (LongWritable value: values
             ) {
            count+=value.get();
        }
        context.write(key,new LongWritable(count));
    }
}
