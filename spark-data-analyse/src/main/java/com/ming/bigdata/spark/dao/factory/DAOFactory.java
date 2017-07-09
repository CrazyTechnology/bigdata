package com.ming.bigdata.spark.dao.factory;

import com.ming.bigdata.spark.dao.ITaskDAO;
import com.ming.bigdata.spark.dao.impl.TaskDAOImpl;

/**
 * Created by root on 7/9/17.
 */
public class DAOFactory {

    /**
     * 私有化够找方法
     */
    private DAOFactory(){}
    public static ITaskDAO getTaskDAO() {
        return new TaskDAOImpl();
    }
}
