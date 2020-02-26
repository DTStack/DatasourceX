package com.dtstack.dtcenter.common.loader.nosql.redis;

import com.dtstack.dtcenter.common.exception.DtCenterDefException;
import com.dtstack.dtcenter.common.loader.nosql.common.AbsNosqlClient;
import com.dtstack.dtcenter.loader.dto.SourceDTO;

public class RedisClientTest {
    private static AbsNosqlClient nosqlClient = new RedisClient();

    @org.junit.Test
    public void testCon() throws Exception {
        SourceDTO source = SourceDTO.builder()
                .url("172.16.10.61:6379")
                .password("abc123")
                .schema("5")
                .build();
        Boolean isConnected = nosqlClient.testCon(source);
        if (!isConnected) {
            throw new DtCenterDefException("数据源连接异常");
        }
    }
}