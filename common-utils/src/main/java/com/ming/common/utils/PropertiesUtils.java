package com.ming.common.utils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * Created by ming on 18-6-19.
 */
public class PropertiesUtils {
    private static final Logger logger= Logger.getLogger("PropertiesUtils");
    public static String getProperties(String name,String path){
        Properties properties=new Properties();
        InputStream in = PropertiesUtils.class.getClassLoader().getResourceAsStream(path);
        try {
            logger.warning("加载配置文件"+path);
            properties.load(in);
            logger.warning("加载配置文件成功"+path);
          return   properties.get(name)==null?null:properties.get(name).toString();
        } catch (IOException e) {
            e.printStackTrace();
            logger.warning("加载配置文件失败"+path);
        }
        return null;
    }
}
