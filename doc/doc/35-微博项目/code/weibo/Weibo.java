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

        //关注
//        WeiBoUtil.addAttends("1001", "1002", "1003");

        //被关注的人发微博（多个人发微博）
//        WeiBoUtil.putData(Contants.CONTENT_TABLE, "1002", "info", "content", "今天天气真晴朗！");
//        WeiBoUtil.putData(Contants.CONTENT_TABLE, "1002", "info", "content", "春困秋乏！");
//        WeiBoUtil.putData(Contants.CONTENT_TABLE, "1003", "info", "content", "夏打盹！");
//        WeiBoUtil.putData(Contants.CONTENT_TABLE, "1001", "info", "content", "冬眠睡不醒！");
        //获取关注人的微博
        WeiBoUtil.getWeiBo("1001");

        //关注已经发过微博的人
//        WeiBoUtil.addAttends("1002", "1001");

        //获取关注人的微博
//        WeiBoUtil.getWeiBo("1002");

        //取消关注
        WeiBoUtil.deleteRelation("1001","1002");

        //获取关注人的微博
        WeiBoUtil.getWeiBo("1001");

    }
}
