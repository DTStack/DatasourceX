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
        } catch (DtLoaderException e) {
            throw e;
        } catch (Exception e) {
            throw new DtLoaderException("方法执行异常！", e);
        } finally {
            Thread.currentThread().setContextClassLoader(oldClassLoader);
        }
    }
}
