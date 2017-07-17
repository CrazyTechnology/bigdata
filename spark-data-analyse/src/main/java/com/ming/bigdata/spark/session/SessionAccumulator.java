package com.ming.bigdata.spark.session;

import org.apache.spark.AccumulatorParam;

/**
 * Created by ming on 2017/7/17.
 */
public class SessionAccumulator implements AccumulatorParam<String> {


    @Override
    public String addAccumulator(String t1, String t2) {
        return addInPlace(t1, t2);
    }

    @Override
    public String addInPlace(String r1, String r2) {
        return null;
    }

    @Override
    public String zero(String initialValue) {
        return null;
    }
}
