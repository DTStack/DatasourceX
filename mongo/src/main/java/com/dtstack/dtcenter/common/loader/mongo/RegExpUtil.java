package com.dtstack.dtcenter.common.loader.mongo;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringEscapeUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @program: data-api-gateway
 * @description:
 * @author: 遥远
 * @create: 2020-05-14 21:06
 */
@Slf4j
public class RegExpUtil {

    static String collectionRegExp = "getCollection\\('(\\w+?)'\\)";
    static String findRegExp = "\\.(find|findOne|aggregate|count|countDocuments|distinct)\\(([\\w\\W]*?)\\)[;|.]";
    static String skipRegExp = "\\.skip\\(([\\w\\W].*?)\\)[;|.]";
    static String limitRegExp = "\\.limit\\(([\\w\\W].*?)\\)[;|.]";
    static String batchSizeRegExp = "\\.batchSize\\(([\\w\\W].*?)\\)[;|.]";
    static String sortRegExp = "\\.sort\\(([\\w\\W].*?)\\)[;|.]";

    public static String getCollectionName(String sql) {
        return GetWithRegExp(sql, collectionRegExp);
    }

    public static void main(String[] args) {
        String sql = "db.getCollection('table_name').find({'age':20});";
        log.info(getQuery(sql));
        log.info(getCollectionName(sql));
    }

    public static String getQuery(String sql) {
        Pattern r = Pattern.compile(findRegExp);
        // 现在创建 matcher 对象
        Matcher m = r.matcher(sql);
        if (m.find()) {
            return m.group(2);
        }
        return null;
    }

    public static String getSkip(String sql) {
        return GetWithRegExp(sql, skipRegExp);
    }

    public static String getLimit(String sql) {
        return GetWithRegExp(sql, limitRegExp);
    }

    public static String getBatchSize(String sql) {
        return GetWithRegExp(sql, batchSizeRegExp);
    }

    public static String getSort(String sql) {
        return GetWithRegExp(sql, sortRegExp);
    }

    private static String GetWithRegExp(String s, String regExp) {
        Pattern r = Pattern.compile(regExp);
        // 现在创建 matcher 对象
        Matcher m = r.matcher(s);
        if (m.find()) {
            return m.group(1);
        }

        return null;
    }

    private static List<String> GetWithRegExps(String s, String regExp) {
        List<String> ss = new ArrayList<String>();
        Pattern r = Pattern.compile(regExp);
        // 现在创建 matcher 对象
        Matcher m = r.matcher(s);
        while (m.find()) {
            ss.add(m.group(1));
        }

        return ss;
    }

    /**
     * 压缩、" -> '
     *
     * @param str
     * @return
     */
    public static String transferred(String str) {
        str = StringEscapeUtils.unescapeJava(str);
        str = str.replaceAll("\r|\n| ", "");
        str = str.trim();
        return str;
    }
}
