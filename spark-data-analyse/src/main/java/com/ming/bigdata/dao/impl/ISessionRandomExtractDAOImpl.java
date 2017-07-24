package com.ming.bigdata.dao.impl;

import com.ming.bigdata.dao.ISessionRandomExtractDAO;
import com.ming.bigdata.domain.SessionRandomExtract;
import com.ming.bigdata.jdbc.JDBCHelper;

/**
 * Created by Administrator on 2017-07-24.
 */
public class ISessionRandomExtractDAOImpl implements ISessionRandomExtractDAO {
    /**
     * 插入session随机抽取
     */
    public void insert(SessionRandomExtract sessionRandomExtract) {
        String sql = "insert into session_random_extract values(?,?,?,?,?)";

        Object[] params = new Object[]{sessionRandomExtract.getTaskid(),
                sessionRandomExtract.getSessionid(),
                sessionRandomExtract.getStartTime(),
                sessionRandomExtract.getEndTime(),
                sessionRandomExtract.getSearchKeywords()
               };

        JDBCHelper jdbcHelper = JDBCHelper.getInstance();
        jdbcHelper.executeUpdate(sql, params);
    }
}
