package com.dtstack.dtcenter.common.loader.redis;

import com.dtstack.dtcenter.loader.client.IRedis;
import com.dtstack.dtcenter.loader.dto.RedisQueryDTO;
import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;

import java.util.List;
import java.util.Map;

public class RedisClientSpecial implements IRedis {
    @Override
    public Map<String, Object> executeQuery(ISourceDTO source, RedisQueryDTO queryDTO) {
        return RedisUtils.executeQuery(source, queryDTO);
    }

    @Override
    public List<String> preViewKey(ISourceDTO source, RedisQueryDTO queryDTO) {
        return RedisUtils.getRedisKeys(source, queryDTO);
    }
}
