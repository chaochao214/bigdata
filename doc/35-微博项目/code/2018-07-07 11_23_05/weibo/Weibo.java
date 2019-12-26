package com.atguigu.weibo;

import java.io.IOException;

public class Weibo {

    public static void init() throws IOException {
        WeiBoUtil.createNamespace(Contants.NAME_SPACE);
        //创建用户关系表
        WeiBoUtil.createTable(Contants.RELATION_TABLE, 1, "attends", "fans");
        //创建微博内容表
        WeiBoUtil.createTable(Contants.CONTENT_TABLE, 1, "info");
        //创建收件箱表
        WeiBoUtil.createTable(Contants.INBOX_TABLE, 100, "info");
    }


    public static void main(String[] args) throws IOException {
//        init();
    }
}
