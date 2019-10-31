package com.atguigu;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class TestHbase {

    private static Connection connection = null;
    private static Admin admin = null;

    static {
        //获取配置信息
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "hadoop102");

        try {
            //获取连接
            connection = ConnectionFactory.createConnection(configuration);
            //获取HBaseAdmin对象
            admin = connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void close(Connection connection, Admin admin) {
        if (connection != null) {
            try {
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        if (admin != null) {
            try {
                admin.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    //判断表是否存在
    private static boolean tableExist(String tableName) throws IOException {
        return admin.tableExists(TableName.valueOf(tableName));
//       HBaseAdmin admin = new HBaseAdmin(configuration);
//        return admin.tableExists(tableName);
    }

    //创建表
    private static void createTable(String tableName, List<String> columnFamilys) throws IOException {

        if (tableExist(tableName)) {
            System.out.println("表" + tableName + "已存在！");
            return;
        }

        //创建表描述器
        HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
        for (String cf : columnFamilys) {
            //创建列描述器
            HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(cf);
            hTableDescriptor.addFamily(hColumnDescriptor);
        }
        try {
            //创建表操作
            admin.createTable(hTableDescriptor);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //插入一条数据
    private static void putData(String tableName, String rowkey, String cf, String cn, String value) throws IOException {

        //获取表对象
        Table table = connection.getTable(TableName.valueOf(tableName));
        //创建put对象
        Put put = new Put(Bytes.toBytes(rowkey));
        //添加列族，列名，值
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(cn), Bytes.toBytes(value));
        //执行put操作
        table.put(put);
        //关闭表连接
        table.close();
    }

    //删除表
    private static void deleteTable(String tableName) throws IOException {

        if (!tableExist(tableName)) {
            System.out.println("表" + tableName + "不存在！");
            return;
        }

        //使表不可用
        admin.disableTable(TableName.valueOf(tableName));
        //删除表操作
        admin.deleteTable(TableName.valueOf(tableName));
    }

    public static void main(String[] args) throws IOException {

        //判断表是否存在
//        System.out.println(tableExist("student"));

        //创建表staff,一个列族
        //        createTable("staff", Collections.singletonList("f1"));

        //创建表staff1，多个列族
//        ArrayList<String> cfs = new ArrayList<>();
//        cfs.add("f1");
//        cfs.add("f2");
//        createTable("staff1", cfs);

        //删除表
        //        deleteTable("staff1");

        //插入一条数据
        putData("staff", "1001", "f3", "name", "wenliang");

        //关闭相应资源
        close(connection, admin);
    }
}
