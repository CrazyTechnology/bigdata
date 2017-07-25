package com.ming.bigdata.dao.impl;

import com.ming.bigdata.dao.ITop10SessionDAO;
import com.ming.bigdata.domain.Top10Session;
import com.ming.bigdata.jdbc.JDBCHelper;

/**
 * top10活跃session的DAO实现
 * @author Administrator
 *
 */
public class ITop10SessionDAOImpl implements ITop10SessionDAO {
    public void insert(Top10Session top10Session) {
        String sql = "insert into top10_category_session values(?,?,?,?)";

        Object[] params = new Object[]{
                top10Session.getTaskid(),
                top10Session.getCategoryid(),
                top10Session.getSessionid(),
                top10Session.getClickCount()};

        JDBCHelper jdbcHelper = JDBCHelper.getInstance();
        jdbcHelper.executeUpdate(sql, params);
    }
}
