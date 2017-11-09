package com.ming.hbase.service.impl;

import com.ming.hbase.dao.HbaseDao;
import com.ming.hbase.dao.impl.HbaseDaoImpl;
import com.ming.hbase.service.UserModuleService;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Map;

/**
 * Created by ming on 2017/10/11.
 */
public class UserModuleServiceImpl implements UserModuleService {
    HbaseDao hbaseDao=new HbaseDaoImpl();
    
    public Map<String, Object> findUserById(String id) throws IOException {
        Result result = hbaseDao.getOneRow("vote_record", id);
        for (KeyValue kv : result.list()) {
            System.out.println("family:" + Bytes.toString(kv.getFamily()));
            System.out
                    .println("qualifier:" + Bytes.toString(kv.getQualifier()));
            System.out.println("value:" + Bytes.toString(kv.getValue()));
            System.out.println("Timestamp:" + kv.getTimestamp());
            System.out.println("-------------------------------------------");
        }
        return null;
    }


    public static void main(String [] args) throws IOException {
        UserModuleServiceImpl service=new UserModuleServiceImpl();
        Map<String, Object> userById = service.findUserById("10");
    }


}
