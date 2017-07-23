package com.ming.bigdata.dao;

import com.ming.bigdata.domain.Task;

/**
 * Created by ming on 2017/7/22.
 */
public interface ITaskDAO {

    Task findById(long taskid);
}
