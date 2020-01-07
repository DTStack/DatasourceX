package com.dtstack.dtcenter.loader;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 15:56 2020/1/6
 */
@FunctionalInterface
public interface ClassLoaderCallBack<T> {
    T execute() throws Exception;
}
