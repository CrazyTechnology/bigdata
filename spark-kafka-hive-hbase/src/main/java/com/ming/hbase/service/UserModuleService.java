package com.ming.hbase.service;

import java.io.IOException;
import java.util.Map;

/**
 * Created by ming on 2017/10/11.
 */
public interface UserModuleService {

    Map<String,Object> findUserById(String id) throws IOException;
}
