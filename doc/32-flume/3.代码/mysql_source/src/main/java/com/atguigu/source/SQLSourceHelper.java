package com.atguigu.source;

import org.apache.flume.Context;
import org.apache.flume.conf.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.sql.*;
import java.text.ParseException;
import java.util.*;

public class SQLSourceHelper {

    private static final Logger LOG = LoggerFactory.getLogger(SQLSourceHelper.class);
    private int runQueryDelay, batchSize;
    private int startFrom, currentIndex, recordSixe = 0;

    private String table,
            columnsToSelect,
            customQuery,
            query,
            defaultCharsetResultSet;

    private Context context;
    private static final int DEFAULT_QUERY_DELAY = 10000;
    private static final int DEFAULT_BATCH_SIZE = 100;
    private static final int DEFAULT_MAX_ROWS = 2;
    private static final int DEFAULT_INCREMENTAL_VALUE = 0;
    private static final String DEFAULT_DELIMITER_ENTRY = ",";
    private static Connection conn = null;
    private static PreparedStatement ps = null;
    private static final String DEFAULT_CHARSET_RESULTSET = "UTF-8";
    private static String connectionURL, connectionUserName, connectionPassword;

    //获取静态资源
    static {
        Properties p = new Properties();
        try {
            p.load(SQLSourceHelper.class.getClassLoader().getResourceAsStream("jdbc.properties"));
            connectionURL = p.getProperty("dbUrl");
            connectionUserName = p.getProperty("dbUser");
            connectionPassword = p.getProperty("dbPassword");
            Class.forName(p.getProperty("dbDriver"));
        } catch (IOException | ClassNotFoundException e) {
            LOG.error(e.toString());
        }
    }

    //获取JDBC连接
    private static Connection InitConnection(String url, String user, String pw) {
        try {
            Connection conn = DriverManager.getConnection(url, user, pw);
            if (conn == null)
                throw new SQLException();
            return conn;
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    SQLSourceHelper(Context context) throws ParseException {
        this.context = context;
        this.table = context.getString("table");
        this.columnsToSelect = context.getString("columns.to.select", "*");
        this.runQueryDelay = context.getInteger("run.query.delay", DEFAULT_QUERY_DELAY);
        this.customQuery = context.getString("custom.query");
        this.batchSize = context.getInteger("batch.size", DEFAULT_BATCH_SIZE);
        this.startFrom = context.getInteger("start.from", DEFAULT_INCREMENTAL_VALUE);
        this.defaultCharsetResultSet = context.getString("default.charset.resultset", DEFAULT_CHARSET_RESULTSET);

        connectionURL = context.getString("connection.url");
        connectionUserName = context.getString("connection.user");
        connectionPassword = context.getString("connection.password");
        conn = InitConnection(connectionURL, connectionUserName, connectionPassword);
        //校验相应的配置信息
        checkMandatoryProperties();
        currentIndex = getStatusDBIndex(startFrom);
        query = buildQuery();
    }

    //校验相应的配置信息
    private void checkMandatoryProperties() {
        if (connectionURL == null) {
            throw new ConfigurationException("connection.url property not set");
        }
        if (table == null && customQuery == null) {
            throw new ConfigurationException("property table not set");
        }
        if (connectionUserName == null) {
            throw new ConfigurationException("connection.user property not set");
        }
        if (connectionPassword == null) {
            throw new ConfigurationException("connection.password property not set");
        }
    }

    //构建sql语句
    private String buildQuery() {
        String sql = "";
        currentIndex = getStatusDBIndex(startFrom);
        LOG.info(currentIndex + "");
        if (customQuery == null) {
            sql = "SELECT " + columnsToSelect + " FROM " + table;
        } else {
            sql = customQuery;
        }
        StringBuilder execSql = new StringBuilder(sql);
        //全量表以id做为offset,如果为最后一次查询id上限不做限定，否则当前offset
        if (!sql.contains("where")) {
            execSql.append(" where ");
            execSql.append("id").append(">").append(currentIndex);
            return execSql.toString();
        } else {
            int length = execSql.toString().length();
            return execSql.toString().substring(0, length - String.valueOf(currentIndex).length()) + currentIndex;
        }
    }

    //执行查询
    List<List<Object>> executeQuery() {
        try {
            //每次执行查询时都要重新生成sql，因为id不同
            customQuery = buildQuery();
            List<List<Object>> results = new ArrayList<>();
            if (ps == null) {
                ps = conn.prepareStatement(customQuery);
            }
            ResultSet result = ps.executeQuery(customQuery);
            while (result.next()) {
                List<Object> row = new ArrayList<>();
                for (int i = 1; i <= result.getMetaData().getColumnCount(); i++) {
                    row.add(result.getObject(i));
                }
                results.add(row);
            }
            LOG.info("execSql:" + customQuery + "\nresultSize:" + results.size());
            return results;
        } catch (SQLException e) {
            LOG.error(e.toString());
            // 重新连接
            conn = InitConnection(connectionURL, connectionUserName, connectionPassword);
        }
        return null;
    }

    //将结果集转化为字符串
    List<String> getAllRows(List<List<Object>> queryResult) {
        List<String> allRows = new ArrayList<>();
        if (queryResult == null || queryResult.isEmpty())
            return allRows;
        StringBuilder row = new StringBuilder();
        for (List<Object> rawRow : queryResult) {
            Object value = null;
            for (Object aRawRow : rawRow) {
                value = aRawRow;
                if (value == null) {
                    row.append(",");
                } else {
                    row.append(aRawRow.toString()).append(",");
                }
            }
            allRows.add(row.toString());
            row = new StringBuilder();
        }
        return allRows;
    }

    //更新offset元数据状态，每次返回结果集后调用。必须记录每次查询的offset值，为程序中断续跑数据时使用，以id为offset
    void updateOffset2DB(int size) {
        //以source_tab做为KEY，如果不存在则插入，存在则更新（每个源表对应一条记录）
        String sql = "insert into flume_meta(source_tab,currentIndex) VALUES('"
                + this.table
                + "','" + (recordSixe += size)
                + "') on DUPLICATE key update source_tab=values(source_tab),currentIndex=values(currentIndex)";
        LOG.info("updateStatus Sql:" + sql);
        execSql(sql);
    }

    //执行sql语句
    private void execSql(String sql) {
        try {
            ps = conn.prepareStatement(sql);
            LOG.info("exec::" + sql);
            ps.execute();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    //获取当前id的offset
    private Integer getStatusDBIndex(int startFrom) {
        String dbIndex = queryOne("select currentIndex from flume_meta where source_tab='" + table + "'");
        if (dbIndex != null) {
            return Integer.parseInt(dbIndex);
        }
        return startFrom;
    }

    //查询一条数据的执行语句(当前offset)
    private String queryOne(String sql) {
        ResultSet result = null;
        try {
            ps = conn.prepareStatement(sql);
            result = ps.executeQuery();
            while (result.next()) {
                return result.getString(1);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    //关闭相关资源
    void close() {
        try {
            ps.close();
            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    int getCurrentIndex() {
        return currentIndex;
    }

    void setCurrentIndex(int newValue) {
        currentIndex = newValue;
    }

    int getRunQueryDelay() {
        return runQueryDelay;
    }

    int getBatchSize() {
        return batchSize;
    }

    String getQuery() {
        return query;
    }

    String getConnectionURL() {
        return connectionURL;
    }

    private boolean isCustomQuerySet() {
        return (customQuery != null);
    }

    Context getContext() {
        return context;
    }

    public String getConnectionUserName() {
        return connectionUserName;
    }

    public String getConnectionPassword() {
        return connectionPassword;
    }

    String getDefaultCharsetResultSet() {
        return defaultCharsetResultSet;
    }

}