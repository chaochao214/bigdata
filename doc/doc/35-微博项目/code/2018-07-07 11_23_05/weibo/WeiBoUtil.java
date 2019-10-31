package com.atguigu.weibo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;

public class WeiBoUtil {

    //获取hbase配置信息
    private static Configuration configuration = HBaseConfiguration.create();
    static {
        configuration.set("hbase.zookeeper.quorum", "192.168.9.102");
    }

    /**
     * 创建命名空间
     */
    public static void createNamespace(String ns) throws IOException {
        //获取hbase管理员对象
        Connection connection = ConnectionFactory.createConnection(configuration);
        Admin admin = connection.getAdmin();

        //构建命名空间描述器
        NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(ns).build();

        //创建namespace
        admin.createNamespace(namespaceDescriptor);

        admin.close();
        connection.close();
    }

    /**
     *创建表
     */
    public static void createTable(String tableName, int versions, String... cfs) throws IOException {

        //获取hbase管理员对象
        Connection connection = ConnectionFactory.createConnection(configuration);
        Admin admin = connection.getAdmin();

        HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
        for (String cf : cfs) {
            HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(cf);
            hColumnDescriptor.setMaxVersions(versions);
            hTableDescriptor.addFamily(hColumnDescriptor);
        }

        admin.createTable(hTableDescriptor);

        admin.close();
        connection.close();
    }

    public static void putData(String tableName, String uid, String cf, String cn, String value) throws IOException {

        Connection connection = ConnectionFactory.createConnection(configuration);
        Table table = connection.getTable(TableName.valueOf(tableName));

        //封装put
        long ts = System.currentTimeMillis();
        String rowkey = uid + ts;
        Put put = new Put(Bytes.toBytes(rowkey));
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(cn), ts, Bytes.toBytes(value));

        //执行操作
        table.put(put);

        table.close();
        connection.close();
    }

    /**
     * 添加关注用户（多个）
     * 1.在用户关系表中，给当前用户添加attends
     * 2.在用户关系表中，给被关注用户添加fans
     * 3.在收件箱表中，给当前用户添加关注用户最近所发微博的rowkey
     */
    public static void addAttends(String uid, String... attends) throws IOException {

        //1.在用户关系表中，给当前用户添加attends
        Connection connection = ConnectionFactory.createConnection(configuration);
        Table table = connection.getTable(TableName.valueOf(Contants.RELATION_TABLE));

        Put attendPut = new Put(Bytes.toBytes(uid));

        //存放被关注用户的添加对象
        ArrayList<Put> puts = new ArrayList<>();

        puts.add(attendPut);

        for (String attend : attends) {
            attendPut.addColumn(Bytes.toBytes("attends"), Bytes.toBytes(attend), Bytes.toBytes(""));
            //2.在用户关系表中，给被关注用户添加fans
            Put put = new Put(Bytes.toBytes(attend));
            put.addColumn(Bytes.toBytes("fans"), Bytes.toBytes(uid), Bytes.toBytes(""));
            puts.add(put);
        }
        table.put(puts);

    }


}
