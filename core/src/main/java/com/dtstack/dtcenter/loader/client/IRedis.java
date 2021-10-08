package com.dtstack.dtcenter.loader.client;

import com.dtstack.dtcenter.loader.dto.RedisQueryDTO;
import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;

import java.util.List;
import java.util.Map;

/**
 * <p>提供 redis 相关操作方法</p>
 *
 * @author ：qianyi
 * date：Created in 上午10:06 2021/8/16
 * company: www.dtstack.com
 */
public interface IRedis {
    /**
     * redis 自定义查询
     *
     * @param source
     * @param queryDTO
     * @return
     */
    Map<String, Object> executeQuery(ISourceDTO source, RedisQueryDTO queryDTO);

    List<String> preViewKey(ISourceDTO source, RedisQueryDTO queryDTO);
}
