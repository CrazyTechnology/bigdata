package com.ming.bigdata.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Created by root on 7/8/17.
 */
public class ConfigureUtil {
    private static Properties prop = new Properties();

    /**
     * 加载配置文件
     */
    static {
        InputStream rs = ConfigureUtil.class.getClassLoader().getResourceAsStream("properties/db.properties");
        try {
            prop.load(rs);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 获取属性值
     * @param key
     * @return 返回属性值
     */
    public static String getProperty(String key) {
        return prop.getProperty(key);
    }



    public static Integer getInteger(String jdbcDatasourceSize) {
        String value = getProperty(jdbcDatasourceSize);
        try{
            return Integer.valueOf(value);
        }catch (Exception e){
            e.printStackTrace();
        }
        return 0;

    }


    public static Boolean getBoolean(String key){
        String property = getProperty(key);
        try{
          return   Boolean.valueOf(property);
        }catch (Exception e){
            e.printStackTrace();
        }
        return  true;

    }



}
