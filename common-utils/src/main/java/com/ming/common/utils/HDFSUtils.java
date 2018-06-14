package com.ming.common.utils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.junit.Before;
import org.junit.Test;
import java.io.FileInputStream;
import java.io.IOException;

/**
 * Created by root on 6/19/17.
 */
public class HDFSUtils {

    /**
     * FileSystem 是HDFS的核心操作类，
     * 它是抽象到，有很多实现类，
     * DistributedFileSystem  是hdfs到实现类
     */
    FileSystem fs=null;

    @Before
    public void init() throws IOException {
        Configuration configuration=new Configuration();
        configuration.set("fs.defaultFS","hdfs://hadoop-master:9000/");
        fs=FileSystem.get(configuration);
    }



    /**
     *上传本地文件到hdfs文件系统
     */
    @Test
    public void uploadLocalFileToHDFS() throws IOException {
        // Path src=new Path("/bigdata/src/jdk-8u131-linux-i586.tar.gz");
        Path dst=new Path("hdfs://hadoop-master:9000/jdk-linux-i586.tar.gz");//注意路径必须在hdfs中是存在到否则会出错，文件自动上到/ 路径下
        // fs.copyFromLocalFile(src,dst);
        FSDataOutputStream os = fs.create(dst);//设置dst地址
        FileInputStream is=new FileInputStream("/bigdata/src/jdk-8u131-linux-i586.tar.gz");  //源数据
        IOUtils.copy(is,os);

    }


    /**
     * 从HDFS下载文件
     */
    @Test
    public void download(String source,String destination) throws IOException {
        Path dst=new Path(destination);
        Path src=new Path(source);
        fs.copyToLocalFile(src,dst);

        //方法二
//        FSDataInputStream is = fs.open(src);
//        FileOutputStream os=new FileOutputStream("/bigdata/src/jdk.tar.gz");
//        IOUtils.copy(is,os);

    }


    @Test
    public void getNodeInfo() throws IOException {
        DistributedFileSystem hdfs= (DistributedFileSystem) fs;
        DatanodeInfo[] dataNodeStats = hdfs.getDataNodeStats();
        for(int i=0;i<dataNodeStats.length;i++){
            System.out.println(dataNodeStats[i].getHostName());
        }
    }



}
