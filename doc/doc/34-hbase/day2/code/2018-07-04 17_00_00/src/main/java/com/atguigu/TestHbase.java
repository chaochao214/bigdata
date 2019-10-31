package com.atguigu;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
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

    //查询一条数据
    private static void getData(String tableName, String rowkey) throws IOException {

        //获取表对象
        Table table = connection.getTable(TableName.valueOf(tableName));

        Get get = new Get(Bytes.toBytes(rowkey));

        Result result = table.get(get);

        Cell[] cells = result.rawCells();

        //打印获得的数据
        for (Cell cell : cells) {
            System.out.println("rowkey:" + Bytes.toString(CellUtil.cloneRow(cell))
                    + ",CF:" + Bytes.toString(CellUtil.cloneFamily(cell))
                    + ",CN:" + Bytes.toString(CellUtil.cloneQualifier(cell))
                    + ",VALUE:" + Bytes.toString(CellUtil.cloneValue(cell)));
        }

        //关闭表连接
        table.close();
    }

    //获取一行数据（指定列族：列）
    private static void getDataByCN(String tableName, String rowkey, String cf, String cn) throws IOException {

        Table table = connection.getTable(TableName.valueOf(tableName));

        Get get = new Get(Bytes.toBytes(rowkey));
//        get.addFamily()//获取一个列族下的所有数据
        get.addColumn(Bytes.toBytes(cf), Bytes.toBytes(cn));
        get.setMaxVersions(3);

        Result result = table.get(get);
        Cell[] cells = result.rawCells();
        //打印获得的数据
        for (Cell cell : cells) {
            System.out.println("rowkey:" + Bytes.toString(CellUtil.cloneRow(cell))
                    + ",CF:" + Bytes.toString(CellUtil.cloneFamily(cell))
                    + ",CN:" + Bytes.toString(CellUtil.cloneQualifier(cell))
                    + ",VALUE:" + Bytes.toString(CellUtil.cloneValue(cell)));
        }
        table.close();
    }

    //全表扫描
    private static void scanData(String tableName) throws IOException {

        //获取table对象
        Table table = connection.getTable(TableName.valueOf(tableName));

        Scan scan = new Scan();

        ResultScanner scanner = table.getScanner(scan);

        for (Result result : scanner) {
            Cell[] cells = result.rawCells();
            //打印获得的数据
            for (Cell cell : cells) {
                System.out.println("rowkey:" + Bytes.toString(CellUtil.cloneRow(cell))
                        + ",CF:" + Bytes.toString(CellUtil.cloneFamily(cell))
                        + ",CN:" + Bytes.toString(CellUtil.cloneQualifier(cell))
                        + ",VALUE:" + Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }
        table.close();
    }

    //删除一条数据
    private static void deletaData(String tableName, String rowKey, String cf, String cn) throws IOException {

        Table table = connection.getTable(TableName.valueOf(tableName));

        //获取Delete对象
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        //给Delete对象添加具体的列族：列
        Delete delete1 = delete.addColumn(Bytes.toBytes(cf), Bytes.toBytes(cn));
//        delete.addFamilyVersion(Bytes.toBytes(cf), System.currentTimeMillis());
        //实际的删除操作
        table.delete(delete);

        table.close();
    }

    //删除多条数据
    private static void deleteDatas(String tableName, String... rowKeys) throws IOException {

        Table table = connection.getTable(TableName.valueOf(tableName));

        ArrayList<Delete> deletes = new ArrayList<>();

        for (String rowKey : rowKeys) {
            Delete delete = new Delete(Bytes.toBytes(rowKey));
            deletes.add(delete);
        }

        table.delete(deletes);

        table.close();
    }


    public static void main(String[] args) throws IOException {

        //判断表是否存在
//        System.out.println(tableExist("student"));
        //创建表staff,一个列族
//         createTable("staff", Collections.singletonList("f1"));
        //创建表staff1，多个列族
//        ArrayList<String> cfs = new ArrayList<>();
//        cfs.add("f1");
//        cfs.add("f2");
//        createTable("staff1", cfs);
        //删除表
//        deleteTable("staff1");
        //插入一条数据
        putData("student", "1001", "f1", "name", "xueba");
        putData("student", "1001", "f1", "name", "xuezha");
        putData("student", "1001", "f1", "name", "xuemei");

        //获取一行数据
//        getData("staff", "1001");
        //获取一行数据（指定列族：列）
        getDataByCN("student", "1001", "f1", "name");

        //扫描整张表
//        scanData("student");

        //删除一行数据
//        deletaData("student", "1001", "f1", "name");

        //删除多行数据
//        deleteDatas("student", "1002", "1003");

        //关闭相应资源
        close(connection, admin);
    }
}
