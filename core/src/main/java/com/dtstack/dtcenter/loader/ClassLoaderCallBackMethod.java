package com.dtstack.dtcenter.loader;

import com.dtstack.dtcenter.loader.exception.DtLoaderException;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 15:56 2020/1/6
 * @Description：反射
 */
public class ClassLoaderCallBackMethod {
    public static <M> M callbackAndReset(ClassLoaderCallBack<M> classLoaderCallBack, ClassLoader toSetClassLoader) {
        ClassLoader oldClassLoader = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(toSetClassLoader);
        try {
            return classLoaderCallBack.execute();
        } catch (Exception e) {
            throw new DtLoaderException(e.getMessage(), e);
        } finally {
            Thread.currentThread().setContextClassLoader(oldClassLoader);
        }
    }
}
