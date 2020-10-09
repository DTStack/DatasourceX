package com.dtstack.dtcenter.loader.functions;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 14:48 2020/8/21
 * @Description：TODO
 */
@FunctionalInterface
public interface Callback<E> {

    /**
     * 执行
     *
     * @param event
     * @return
     */
    Object submit(E event);
}
