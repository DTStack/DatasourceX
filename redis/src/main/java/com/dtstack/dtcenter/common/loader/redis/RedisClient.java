package com.dtstack.dtcenter.common.loader.redis;

import com.dtstack.dtcenter.common.loader.common.nosql.AbsNoSqlClient;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;

import java.util.List;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 15:38 2020/2/4
 * @Description：Redis 客户端
 */
public class RedisClient<T> extends AbsNoSqlClient<T> {
    @Override
    public Boolean testCon(ISourceDTO iSource) {
        return RedisUtils.checkConnection(iSource);
    }

    @Override
    public List<List<Object>> getPreview(ISourceDTO source, SqlQueryDTO queryDTO) {
        return RedisUtils.getPreview(source, queryDTO);
    }
}
