package com.ming.hadoop.flow;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by root on 6/18/17.
 * 序列化需要实现writable接口
 */
public class FlowBean implements WritableComparable<FlowBean> {

    private String phoneNumber;  //手机号
    private long up_flow;//上行流量
    private long down_flow;//下行流量
    private long sum_flow;//总流量

    public FlowBean() {
    }

    public FlowBean(String phoneNumber, long up_flow, long down_flow) {
        this.phoneNumber = phoneNumber;
        this.up_flow = up_flow;
        this.down_flow = down_flow;
        this.sum_flow = up_flow+down_flow;
    }

    public String getPhoneNumber() {
        return phoneNumber;
    }

    public void setPhoneNumber(String phoneNumber) {
        this.phoneNumber = phoneNumber;
    }

    public long getUp_flow() {
        return up_flow;
    }

    public void setUp_flow(long up_flow) {
        this.up_flow = up_flow;
    }

    public long getDown_flow() {
        return down_flow;
    }

    public void setDown_flow(long down_flow) {
        this.down_flow = down_flow;
    }

    public long getSum_flow() {
        return sum_flow;
    }

    public void setSum_flow(long sum_flow) {
        this.sum_flow = sum_flow;
    }

    //序列化方法，将对象序列化到流中
    public void write(DataOutput out) throws IOException {
        out.writeUTF(phoneNumber); //手机号
        out.writeLong(up_flow);//上行流量
        out.writeLong(down_flow);
        out.writeLong(sum_flow);
    }

    //反序列化方法
    public void readFields(DataInput in) throws IOException {
        String phoneNumber = in.readUTF();
         up_flow = in.readLong();
         down_flow = in.readLong();
         sum_flow = in.readLong();
    }


    @Override
    public String toString() {
        return
                "phoneNumer="+phoneNumber+
                "up_flow=" + up_flow +
                "down_flow=" + down_flow +
                "sum_flow=" + sum_flow ;

    }



    public int compareTo(FlowBean o) {
       return sum_flow>o.getDown_flow()?-1:1;  //倒序排列 -1在前1在后
    }
}
