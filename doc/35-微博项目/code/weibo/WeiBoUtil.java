package com.atguigu.weibo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

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
     * 创建表
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
        String rowkey = uid + "_" + ts;
        Put put = new Put(Bytes.toBytes(rowkey));
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(cn), ts, Bytes.toBytes(value));

        //执行操作
        table.put(put);

        //更新收件箱表
        Table inboxTable = connection.getTable(TableName.valueOf(Contants.INBOX_TABLE));
        Table relationTable = connection.getTable(TableName.valueOf(Contants.RELATION_TABLE));

        Get get = new Get(Bytes.toBytes(uid));
        Result result = relationTable.get(get);

        ArrayList<Put> puts = new ArrayList<>();

        for (Cell cell : result.rawCells()) {
            if ("fans".equals(Bytes.toString(CellUtil.cloneFamily(cell)))) {
                byte[] inboxRowkey = CellUtil.cloneQualifier(cell);

                Put inboxPut = new Put(inboxRowkey);
                inboxPut.addColumn(Bytes.toBytes("info"), Bytes.toBytes(uid), ts, Bytes.toBytes(rowkey));

                puts.add(inboxPut);
            }
        }
        inboxTable.put(puts);

        table.close();
        inboxTable.close();
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

        //3.在收件箱表中，给当前用户添加关注用户最近所发微博的rowkey
        Table inboxTabel = connection.getTable(TableName.valueOf(Contants.INBOX_TABLE));
        Table contentTable = connection.getTable(TableName.valueOf(Contants.CONTENT_TABLE));

        Put inboxPut = new Put(Bytes.toBytes(uid));

        if (attends.length <= 0) {
            return;
        }

        //循环添加要增加的数据
        for (String attend : attends) {
            //通过startRow和stopRow构建扫描器
//            Scan scan = new Scan(Bytes.toBytes(attend), Bytes.toBytes(attend + "|"));
            //通过过滤器构建扫描器
            Scan scan = new Scan();
            RowFilter rowFilter = new RowFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator(attend + "_"));
            scan.setFilter(rowFilter);

            //获取所有符合扫描规则的额数据
            ResultScanner scanner = contentTable.getScanner(scan);

            //循环遍历取出每条数据的rowkey添加到inboxPut中
            for (Result result : scanner) {
                byte[] row = result.getRow();
                inboxPut.addColumn(Bytes.toBytes("info"), Bytes.toBytes(attend), row);
                //往收件箱表中给操作者添加数据
                inboxTabel.put(inboxPut);
            }
        }

        //关闭资源
        inboxTabel.close();
        contentTable.close();
        table.close();
        connection.close();
    }


    /**
     * 取关
     * 1.在用户关系表中，删除当前用户的attends
     * 2.在用户关系表中，删除被取关用户的fans（操作者）
     * 3.在收件箱表中删除取关用户的所有数据
     */
    public static void deleteRelation(String uid, String... deletes) throws IOException {

        Connection connection = ConnectionFactory.createConnection(configuration);
        //获取用户关系表对象
        Table relationTable = connection.getTable(TableName.valueOf(Contants.RELATION_TABLE));

        //存放关系表中所有要输出的对象的集合
        ArrayList<Delete> deleteArrayList = new ArrayList<>();

        // 1.在用户关系表中，删除当前用户的attends
        Delete userDelete = new Delete(Bytes.toBytes(uid));
        for (String delete : deletes) {
            //给当前用户添加要删除的列
            userDelete.addColumn(Bytes.toBytes("attends"), Bytes.toBytes(delete));

            //2.在用户关系表中，删除被取关用户的fans（操作者）
            Delete fanDelete = new Delete(Bytes.toBytes(delete));
            //给被关注这添加删除的列
            fanDelete.addColumn(Bytes.toBytes("fans"), Bytes.toBytes(uid));
            deleteArrayList.add(fanDelete);
        }
        deleteArrayList.add(userDelete);

        //用户关系表删除操作
        relationTable.delete(deleteArrayList);

        //3.在收件箱表中删除取关用户的所有数据
        Table inboxTable = connection.getTable(TableName.valueOf(Contants.INBOX_TABLE));
        Delete inboxDelete = new Delete(Bytes.toBytes(uid));

        //循环添加要删除内容
        for (String delete : deletes) {
            inboxDelete.addColumns(Bytes.toBytes("info"), Bytes.toBytes(delete));
        }
        inboxTable.delete(inboxDelete);

        //关闭资源
        relationTable.close();
        inboxTable.close();
        connection.close();
    }

    /**
     * 获取关注的人的微博内容
     */
    public static void getWeiBo(String uid) throws IOException {

        //获取微博内容表及收件箱表对象
        Connection connection = ConnectionFactory.createConnection(configuration);
        Table inboxTable = connection.getTable(TableName.valueOf(Contants.INBOX_TABLE));
        Table contentTable = connection.getTable(TableName.valueOf(Contants.CONTENT_TABLE));

        Get get = new Get(Bytes.toBytes(uid));
        get.setMaxVersions(3);

        Result result = inboxTable.get(get);
        for (Cell cell : result.rawCells()) {
            byte[] contentRowkey = CellUtil.cloneValue(cell);
            Get contentGet = new Get(contentRowkey);
            Result contentResult = contentTable.get(contentGet);
            for (Cell cell1 : contentResult.rawCells()) {
                String uid_ts = Bytes.toString(CellUtil.cloneRow(cell1));
                String id = uid_ts.split("_")[0];
                String ts = uid_ts.split("_")[1];

                String date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(Long.parseLong(ts)));
                System.out.println("用户：" + id + "，时间" + date + "，内容：" + Bytes.toString(CellUtil.cloneValue(cell1)));
            }
        }

        inboxTable.close();
        contentTable.close();
        connection.close();
    }

}
