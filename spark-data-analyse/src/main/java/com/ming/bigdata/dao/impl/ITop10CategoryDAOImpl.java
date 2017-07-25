package com.ming.bigdata.dao.impl;

import com.ming.bigdata.dao.ITop10CategoryDAO;
import com.ming.bigdata.domain.Top10Category;
import com.ming.bigdata.jdbc.JDBCHelper;

/**
 * Created by ming on 2017/7/24.
 */
public class ITop10CategoryDAOImpl implements ITop10CategoryDAO {
    public void insert(Top10Category category) {
        String sql = "insert into top10_category values(?,?,?,?,?)";

        Object[] params = new Object[]{category.getTaskid(),
                category.getCategoryid(),
                category.getClickCount(),
                category.getOrderCount(),
                category.getPayCount()};

        JDBCHelper jdbcHelper = JDBCHelper.getInstance();
        jdbcHelper.executeUpdate(sql, params);
    }
}
