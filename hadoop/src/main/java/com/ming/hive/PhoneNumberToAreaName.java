package com.ming.hive;

import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.HashMap;

/**
 * Created by root on 6/25/17.
 */
public class PhoneNumberToAreaName extends UDF {
    private static HashMap<String,String> areaMap=new HashMap<String, String>();
    static{
        areaMap.put("138","beijing");
        areaMap.put("139","tianjin");
        areaMap.put("136","dongjing");
    }

    public String  evaluate(String phoneNumber){
        String result=areaMap.get(phoneNumber.substring(0,3)==null?phoneNumber+"huoxing":(phoneNumber+" "+areaMap.get(phoneNumber.substring(0,3))));
        return result;

    }


}
