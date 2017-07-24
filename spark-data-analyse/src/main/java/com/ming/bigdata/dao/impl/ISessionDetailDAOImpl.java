package com.ming.bigdata.dao.impl;

import com.ming.bigdata.dao.ISessionDetailDAO;
import com.ming.bigdata.domain.SessionDetail;
import com.ming.bigdata.jdbc.JDBCHelper;

/**
 * Created by Administrator on 2017-07-24.
 */
public class ISessionDetailDAOImpl implements ISessionDetailDAO {
    /**
     * 插入一条session明细数据
     * @param sessionDetail
     */
    public void insert(SessionDetail sessionDetail) {
        String sql = "insert into session_detail values(?,?,?,?,?,?,?,?,?,?,?,?)";

        Object[] params = new Object[]{
                sessionDetail.getTaskid(),
                sessionDetail.getUserid(),
                sessionDetail.getSessionid(),
                sessionDetail.getPageid(),
                sessionDetail.getActionTime(),
                sessionDetail.getSearchKeyword(),
                sessionDetail.getClickCategoryId(),
                sessionDetail.getClickProductId(),
                sessionDetail.getOrderCategoryIds(),
                sessionDetail.getOrderProductIds(),
                sessionDetail.getPayCategoryIds(),
                sessionDetail.getPayProductIds()};

        JDBCHelper jdbcHelper = JDBCHelper.getInstance();
        jdbcHelper.executeUpdate(sql, params);
    }

}
