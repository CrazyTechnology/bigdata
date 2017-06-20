package com.ming.hadoop.flow;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by root on 6/18/17.
 */
public class FlowRedcer extends Reducer<Text,FlowBean,Text,FlowBean> {



    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {

        long up_flow_counter=0;
        long down_flow_counter=0;
        for (FlowBean flow: values
             ) {
            up_flow_counter+=flow.getUp_flow();
            down_flow_counter+=flow.getDown_flow();
        }
        context.write(key,new FlowBean(key.toString(),up_flow_counter,down_flow_counter));

    }
}
