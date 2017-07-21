package com.ming.bigdata.dao;

import com.ming.bigdata.domain.Task;

/**
 * Created by root on 7/9/17.
 */
public interface ITaskDAO {

    //查询任务详情
    public Task findById(long taskid);
}
