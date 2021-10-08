package com.dtstack.dtcenter.loader.client.redis;

import com.dtstack.dtcenter.loader.ClassLoaderCallBackMethod;
import com.dtstack.dtcenter.loader.client.IRedis;
import com.dtstack.dtcenter.loader.dto.RedisQueryDTO;
import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;

import java.util.List;
import java.util.Map;

public class RedisProxy implements IRedis {

    IRedis targetClient = null;

    public RedisProxy(IRedis iRedis) {
        this.targetClient = iRedis;
    }

    @Override
    public Map<String, Object> executeQuery(ISourceDTO source, RedisQueryDTO queryDTO) {
        return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.executeQuery(source, queryDTO),
                targetClient.getClass().getClassLoader());
    }

    @Override
    public List<String> preViewKey(ISourceDTO source, RedisQueryDTO queryDTO) {
        return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.preViewKey(source, queryDTO),
                targetClient.getClass().getClassLoader());
    }


}
