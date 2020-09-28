package com.dtstack.dtcenter.loader;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 15:56 2020/1/6
 * @Description：反射
 */
public class ClassLoaderCallBackMethod {
    public static <M> M callbackAndReset(ClassLoaderCallBack<M> classLoaderCallBack, ClassLoader toSetClassLoader) throws Exception {
        ClassLoader oldClassLoader = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(toSetClassLoader);
        try {
            return classLoaderCallBack.execute();
        } finally {
            Thread.currentThread().setContextClassLoader(oldClassLoader);
        }
    }
}
