package com.ming.bigdata.dao;

import com.ming.bigdata.dao.impl.ISessionAggrStatDAOImpl;
import com.ming.bigdata.dao.impl.ISessionDetailDAOImpl;
import com.ming.bigdata.dao.impl.ISessionRandomExtractDAOImpl;
import com.ming.bigdata.dao.impl.ITaskDaoImpl;

/**
 * Created by ming on 2017/7/22.
 */
public class DAOFactory {

    public static ITaskDAO getTaskDAO() {
        return new ITaskDaoImpl();
    }

    public static ISessionAggrStatDAO getSessionAggrStatDAO() {
        return new ISessionAggrStatDAOImpl();
    }

    public static ISessionRandomExtractDAO getSessionRandomExtractDAO() {
        return new ISessionRandomExtractDAOImpl();
    }

    public static ISessionDetailDAO getSessionDetailDAO() {
        return  new ISessionDetailDAOImpl();
    }
}
