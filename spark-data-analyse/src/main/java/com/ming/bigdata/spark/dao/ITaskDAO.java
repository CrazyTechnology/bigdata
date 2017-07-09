package com.ming.bigdata.spark.dao;

import com.ming.bigdata.spark.domain.Task;

/**
 * Created by root on 7/9/17.
 */
public interface ITaskDAO {

    //查询任务详情
    public Task findById(long taskid);
}
