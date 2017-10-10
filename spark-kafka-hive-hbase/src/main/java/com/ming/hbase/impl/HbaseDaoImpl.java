package com.ming.hbase.impl;

import com.ming.hbase.HbaseDao;
import com.ming.util.ConfigurationManagement;
import com.ming.util.Constants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.PrefixFilter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by ming on 2017/10/10.
 */
public class HbaseDaoImpl implements HbaseDao {
    // 声明静态配置
    private static Configuration configuration = null;
    private static HBaseAdmin hBaseAdmin = null;
    private static Connection connection = null;
    static {
        configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", ConfigurationManagement.getProperty(Constants.ZOOKEEPER_LIST));
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        configuration.set("hbase.master",ConfigurationManagement.getProperty(Constants.HBASE_MASTER));
        try {
            connection = ConnectionFactory.createConnection(configuration);
            // 创建一个数据库管理员
            hBaseAdmin = (HBaseAdmin) connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }


    public void save(Put put, String tableName) throws IOException {
        // TODO Auto-generated method stub
        Table table = connection.getTable(TableName.valueOf(tableName));
        table.put(put);
        table.close();
    }

    public void insert(String tableName, String rowKey, String family, String quailifer, String value) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Put put = new Put(rowKey.getBytes());
        put.addColumn(family.getBytes(), quailifer.getBytes(), value.getBytes());
        table.put(put);
        table.close();
    }


    public void insert(String tableName, String rowKey, String family, String[] quailifer, String[] value) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Put put = new Put(rowKey.getBytes());
        // 批量添加
        for (int i = 0; i < quailifer.length; i++) {
            String col = quailifer[i];
            String val = value[i];
            put.addColumn(family.getBytes(), col.getBytes(), val.getBytes());
        }
        table.put(put);
        table.close();
    }


    public void save(List<Put> Put, String tableName) throws IOException {
        // TODO Auto-generated method stub
        Table table = connection.getTable(TableName.valueOf(tableName));
        table.put(Put);
        table.close();
    }

    public Result getOneRow(String tableName, String rowKey) throws IOException {
        // TODO Auto-generated method stub
        Table table = connection.getTable(TableName.valueOf(tableName));
        Get get = new Get(rowKey.getBytes());
        Result rsResult = table.get(get);
        table.close();
        return rsResult;
    }

    public List<Result> getRows(String tableName, String rowKey_like) throws IOException {
        // TODO Auto-generated method stub
        Table table = connection.getTable(TableName.valueOf(tableName));
        ;
        PrefixFilter filter = new PrefixFilter(rowKey_like.getBytes());
        Scan scan = new Scan();
        scan.setFilter(filter);
        ResultScanner scanner = table.getScanner(scan);
        List<Result> list = new ArrayList<Result>();
        for (Result rs : scanner) {
            list.add(rs);
        }
        table.close();
        return list;
    }

    public List<Result> getRows(String tableName, String rowKeyLike, String[] cols) throws IOException {
        // TODO Auto-generated method stub
        Table table = connection.getTable(TableName.valueOf(tableName));
        PrefixFilter filter = new PrefixFilter(rowKeyLike.getBytes());

        Scan scan = new Scan();
        for (int i = 0; i < cols.length; i++) {
            scan.addColumn("cf".getBytes(), cols[i].getBytes());
        }
        scan.setFilter(filter);
        ResultScanner scanner = table.getScanner(scan);
        ArrayList<Result> list = new ArrayList<Result>();
        ;
        for (Result rs : scanner) {
            list.add(rs);
        }
        table.close();
        return list;
    }

    public List<Result> getRows(String tableName, String startRow, String stopRow) throws IOException {

        Table table = connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        scan.setStartRow(startRow.getBytes());
        scan.setStopRow(stopRow.getBytes());
        ResultScanner scanner = table.getScanner(scan);
        List<Result> list = new ArrayList<Result>();
        for (Result rsResult : scanner) {
            list.add(rsResult);
        }
        table.close();
        return list;

    }

    public void deleteRecords(String tableName, String rowKeyLike) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        PrefixFilter filter = new PrefixFilter(rowKeyLike.getBytes());
        Scan scan = new Scan();
        scan.setFilter(filter);
        ResultScanner scanner = table.getScanner(scan);
        List<Delete> list = new ArrayList<Delete>();
        for (Result rs : scanner) {
            Delete del = new Delete(rs.getRow());
            list.add(del);
        }
        table.delete(list);
        table.close();
    }

    public void deleteTable(String tableName) throws IOException {

        if (hBaseAdmin.tableExists(tableName)) {
            hBaseAdmin.disableTable(tableName);// 禁用表
            hBaseAdmin.deleteTable(tableName);// 删除表
            System.err.println("删除表成功!");
        } else {
            System.err.println("删除的表不存在！");
        }
        //  hAdmin.close();
    }

    public  String createTable(String tableName, String[] columnFamilys) throws IOException {

        if (hBaseAdmin.tableExists(tableName)) {
            return "table existed";
        } else {
            HTableDescriptor tableDesc = new HTableDescriptor(
                    TableName.valueOf(tableName));
            for (String columnFamily : columnFamilys) {
                tableDesc.addFamily(new HColumnDescriptor(columnFamily));
            }
            hBaseAdmin.createTable(tableDesc);
            return "success";
        }
        //hBaseAdmin.close();// 关闭释放资源
    }

    public List<Result> getRowsByOneKey(String tableName, String rowKeyLike, String[] cols) throws IOException {
        // TODO Auto-generated method stub
        Table table = connection.getTable(TableName.valueOf(tableName));
        PrefixFilter filter = new PrefixFilter(rowKeyLike.getBytes());

        Scan scan = new Scan();
        for (int i = 0; i < cols.length; i++) {
            scan.addColumn("cf".getBytes(), cols[i].getBytes());
        }
        scan.setFilter(filter);
        ResultScanner scanner = table.getScanner(scan);
        List<Result> list = new ArrayList<Result>();
        for (Result rs : scanner) {
            list.add(rs);
        }
        table.close();
        return list;
    }

    public Result getOneRowAndMultiColumn(String tableName, String rowKey, String[] cols) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Get get = new Get(rowKey.getBytes());
        for (int i = 0; i < cols.length; i++) {
            get.addColumn("cf".getBytes(), cols[i].getBytes());
        }
        Result rsResult = table.get(get);
        table.close();
        return rsResult;
    }


    public static void main(String args[]) throws IOException {
        HbaseDao hbaseDao=new HbaseDaoImpl();
        String[] columnFamilys=new String[]{"info"};
        hbaseDao.createTable("bigdata",columnFamilys);
    }
}
