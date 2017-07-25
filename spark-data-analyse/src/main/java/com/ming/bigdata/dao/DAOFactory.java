package com.ming.bigdata.dao;

import com.ming.bigdata.dao.impl.*;

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

    public static ITop10CategoryDAO getTop10CategoryDAO() {
        return new ITop10CategoryDAOImpl();
    }

    public static ITop10SessionDAO getTop10SessionDAO() {
        return  new ITop10SessionDAOImpl();
    }
}
