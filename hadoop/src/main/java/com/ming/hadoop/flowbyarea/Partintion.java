package com.ming.hadoop.flowbyarea;

import org.apache.hadoop.mapreduce.Partitioner;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by root on 6/19/17.
 */
public class Partintion<Key,Value> extends Partitioner<Key,Value> {

    private static Map areaMap=new HashMap<String,Integer>();

    static{
        //load data
        areaMap.put("134",0);
        areaMap.put("133",1);
        areaMap.put("135",2);
        areaMap.put("136",3);
        areaMap.put("137",4);
        areaMap.put("152",5);
        areaMap.put("153",6);
        areaMap.put("135",7);
    }


    @Override
    public int getPartition(Key key, Value value, int i) {
      int code= areaMap.get(key.toString().substring(0,3))==null?8: Integer.parseInt(areaMap.get(key.toString().substring(0, 3)).toString());
        return code;
    }
}
