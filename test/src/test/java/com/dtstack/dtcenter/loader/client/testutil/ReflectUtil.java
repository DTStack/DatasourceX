package com.dtstack.dtcenter.loader.client.testutil;

import java.lang.reflect.Method;

/**
 * 反射工具类
 *
 * @author ：wangchuan
 * date：Created in 上午10:56 2021/6/4
 * company: www.dtstack.com
 */
public class ReflectUtil {

    @SuppressWarnings("unchecked")
    public static <T> T invokeMethod(Object obj, Class<T> returnType , String methodName, Class<?> [] parameterType, Object... args) {
        try {
            Method method = obj.getClass().getDeclaredMethod(methodName, parameterType);
            method.setAccessible(true);
            return (T) method.invoke(obj, args);
        } catch (Exception e) {
            throw new RuntimeException(String.format("invoke method error: %s", e.getMessage()), e);
        }
    }

    @SuppressWarnings("unchecked")
    public static <T> T invokeStaticMethod(Class<?> source, Class<T> returnType , String methodName, Class<?> [] parameterType, Object... args) {
        try {
            Method method = source.getDeclaredMethod(methodName, parameterType);
            method.setAccessible(true);
            return (T) method.invoke(source.newInstance(), args);
        } catch (Exception e) {
            throw new RuntimeException(String.format("invoke method error: %s", e.getMessage()), e);
        }
    }
}
