package com.dtstack.dtcenter.common.loader.common.utils;

import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import org.apache.commons.lang3.StringUtils;

import java.lang.reflect.Field;
import java.util.Objects;

/**
 * 反射工具类
 *
 * @author ：wangchuan
 * date：Created in 上午10:18 2021/5/21
 * company: www.dtstack.com
 */
public class ReflectUtil {

    /**
     * 判断类中指定字段是否存在
     *
     * @param c         class 类型
     * @param fieldName 字段名称
     * @param <T>       对象类型
     * @return 是否存在
     */
    public static <T> Boolean fieldExists(Class<T> c, String fieldName) {
        if (Objects.isNull(c) || StringUtils.isBlank(fieldName)) {
            throw new DtLoaderException("class or fieldName can not be null...");
        }
        Field[] fields = c.getDeclaredFields();
        for (Field field : fields) {
            if (fieldName.equals(field.getName())) {
                return true;
            }
        }
        return false;
    }

    /**
     * 重新设置静态/非静态变量字段属性
     *
     * @param c           class 类型
     * @param fieldName   字段名
     * @param obj         对象
     * @param targetParam 目标值
     * @param <T>         对象类型
     */
    public static <T> void setField(Class<T> c, String fieldName, Object obj, Object targetParam) {
        try {
            Field field = c.getDeclaredField(fieldName);
            field.setAccessible(true);
            field.set(obj, targetParam);
        } catch (Exception e) {
            throw new DtLoaderException(String.format("set class: %s field: %s fail", c.getName(), fieldName));
        }
    }
}
