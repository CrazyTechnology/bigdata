package com.ming.bigdata.spark.dao.factory;

import com.ming.bigdata.spark.dao.ITaskDAO;
import com.ming.bigdata.spark.dao.impl.TaskDAOImpl;
import com.ming.bigdata.spark.session.ISessionAggrStatDAO;
import com.ming.bigdata.spark.session.SessionAggrStatDAOImpl;

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

    public static ISessionAggrStatDAO getSessionAggrStatDAO() {
        return new SessionAggrStatDAOImpl();
    }
}
