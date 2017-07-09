package com.ming.bigdata.spark.util;

/**
 * Created by root on 7/8/17.
 */
public class Singleton {

    public static Singleton  instance=null;

    private Singleton(){}

    public static Singleton getInstance(){
        if (instance==null){
            synchronized(Singleton.class) {
                if(instance==null)
                    instance = new Singleton();
            }
        }
        return  instance;
    }
}
