package cn.qzt.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

/**
 * Title:  hadoopdemo
 * Description:  SSD
 *
 * @author dc
 * @date2018/7/1318:49
 */
public class HBaseApp {
    Connection connection = null;
    Table table = null;
    Admin admin = null;

    @Before
    public void setUp() throws IOException {
        Configuration configuration = new Configuration();
        configuration.set("hbase.rootdir","hdfs://hadoop123:8020/hbase");
        configuration.set("hbase.zookeeper.quorum","hadoop123:2181");
        connection = ConnectionFactory.createConnection(configuration);
        admin = connection.getAdmin();
    }

    /**
     * 查看表
     */
    @Test
    public void queryTable()throws Exception{
        HTableDescriptor[] hTableDescriptors = admin.listTables();
        if(hTableDescriptors.length>0){
            for(HTableDescriptor hd :hTableDescriptors ){
                //获取表名
                System.out.println(hd.getNameAsString());
                for(HColumnDescriptor hColumnDescriptor:hd.getColumnFamilies()){

                    System.out.println(hColumnDescriptor.getNameAsString());

                }
            }
        }



    }




    @After
    public void tearDown()throws Exception{
        connection.close();

    }
}
