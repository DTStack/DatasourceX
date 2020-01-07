package com.dtstack.dtcenter.loader.utils;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 17:09 2020/1/3
 * @Description：数字转换
 */
public class MathUtil {
    public static String getString(Object obj){
        if(obj == null){
            return null;
        }

        if(obj instanceof String){
            return (String) obj;
        }else{
            return obj.toString();
        }

    }

    public static String getString(Object obj, String defaultVal){
        if(obj == null){
            return defaultVal;
        }

        return getString(obj);
    }
}
