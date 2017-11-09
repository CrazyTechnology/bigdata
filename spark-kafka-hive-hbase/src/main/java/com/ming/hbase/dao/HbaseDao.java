package com.ming.hbase.dao;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;

import java.io.IOException;
import java.util.List;

/**
 * Created by ming on 2017/10/10.
 */
public interface HbaseDao {
    public void deleteTable(String tableName) throws IOException;
    public void save(Put put, String tableName) throws IOException;
    public void insert(String tableName, String rowKey, String family, String quailifer, String value) throws IOException;
    public void insert(String tableName, String rowKey, String family, String[] quailifer, String[] value) throws IOException;
    public void save(List<Put> Put, String tableName) throws IOException;
    public Result getOneRow(String tableName, String rowKey) throws IOException;
    public List<Result> getRows(String tableName, String rowKey_like) throws IOException;
    public List<Result> getRows(String tableName, String rowKeyLike, String[] cols) throws IOException;
    public String createTable(String tableName, String[] columnFamilys) throws IOException;

}
