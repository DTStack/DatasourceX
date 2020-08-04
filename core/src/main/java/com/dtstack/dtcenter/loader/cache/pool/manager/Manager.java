package com.dtstack.dtcenter.loader.cache.pool.manager;

import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;

/**
 * @company: www.dtstack.com
 * @Author ：wangchuan
 * @Date ：Created in 下午3:27 2020/8/3
 * @Description：自定义连接池管理接口
 */
public interface Manager<T> {

    T getConnection(ISourceDTO source) throws Exception;

    T initSource(ISourceDTO source);
}
