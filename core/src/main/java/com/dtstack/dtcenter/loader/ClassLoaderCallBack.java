package com.dtstack.dtcenter.loader;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 15:56 2020/1/6
 */
@FunctionalInterface
public interface ClassLoaderCallBack<T> {
    /**
     * 抽象执行类
     * @return
     * @throws Exception
     */
    T execute() throws Exception;
}
