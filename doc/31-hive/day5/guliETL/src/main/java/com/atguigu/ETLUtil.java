package com.atguigu;

/**
 * 1.过滤脏数据
 * 2.去掉类别字段中空格
 * 3.替换关联视频的分割符
 */
public class ETLUtil {

    public static String etlStr(String line) {

        //切割数据
        String[] split = line.split("\t");
        // * 1.过滤脏数据
        if (split.length < 9) return null;
        // 2.去掉类别字段中空格
        split[3] = split[3].replaceAll(" ", "");
        //3.替换关联视频的分割符
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < split.length; i++) {
            if (i < 9) {
                if (i == split.length - 1) {
                    sb.append(split[i]);
                } else {
                    sb.append(split[i]).append("\t");
                }
            } else {
                if (i == split.length - 1) {
                    sb.append(split[i]);
                } else {
                    sb.append(split[i]).append("&");
                }
            }

        }
        return sb.toString();
    }

}
