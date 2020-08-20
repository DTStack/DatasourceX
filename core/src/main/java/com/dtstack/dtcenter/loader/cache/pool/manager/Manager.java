package com.dtstack.dtcenter.loader.cache.pool.manager;

import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;

/**
 * @company: www.dtstack.com
 * @Author ：wangchuan
 * @Date ：Created in 下午3:27 2020/8/3
 * @Description：自定义连接池管理接口
 */
public interface Manager<T> {

    /**
     * 获取连接
     *
     * @param source
     * @return
     * @throws Exception
     */
    T getConnection(ISourceDTO source) throws Exception;

    /**
     * 初始化数据源连接池
     *
     * @param source
     * @return
     */
    T initSource(ISourceDTO source);
}
