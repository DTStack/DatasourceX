package com.dtstack.dtcenter.common.loader.redis;

import com.dtstack.dtcenter.common.exception.DtCenterDefException;
import com.dtstack.dtcenter.loader.dto.source.RedisSourceDTO;

public class RedisClientTest {
    private static RedisClient nosqlClient = new RedisClient();

    @org.junit.Test
    public void testCon() throws Exception {
        RedisSourceDTO source = RedisSourceDTO.builder()
                .hostPort("172.16.8.109:6379")
                .schema("5")
                .build();
        Boolean isConnected = nosqlClient.testCon(source);
        if (!isConnected) {
            throw new DtCenterDefException("数据源连接异常");
        }
    }
}