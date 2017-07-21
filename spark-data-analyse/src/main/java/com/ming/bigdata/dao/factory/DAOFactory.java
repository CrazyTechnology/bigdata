package com.ming.bigdata.dao.factory;

import com.ming.bigdata.dao.impl.TaskDAOImpl;
import com.ming.bigdata.dao.ITaskDAO;

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
