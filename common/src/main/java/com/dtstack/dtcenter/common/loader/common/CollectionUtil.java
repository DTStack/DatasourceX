package com.dtstack.dtcenter.common.loader.common;

import org.apache.commons.lang3.StringUtils;

import java.util.List;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 20:24 2020/2/26
 * @Description：数组工具类
 */
public class CollectionUtil {
    public static String listToStr(List list) {
        StringBuilder sBuilder = new StringBuilder();
        list.stream().filter(str -> {
            if (StringUtils.isBlank((String) str)) {
                return false;
            } else {
                return true;
            }
        }).forEach(str -> {
            sBuilder.append(str).append(",");
        });
        if (sBuilder.length() > 0) {
            sBuilder.deleteCharAt(sBuilder.length() - 1);
        }
        return sBuilder.toString();
    }
}
