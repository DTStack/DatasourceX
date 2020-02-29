package com.dtstack.dtcenter.common.loader.nosql.mongo;

import com.dtstack.dtcenter.common.exception.DtCenterDefException;
import com.dtstack.dtcenter.common.loader.nosql.common.AbsNosqlClient;
import com.dtstack.dtcenter.loader.dto.SourceDTO;
import org.junit.Test;

public class MongoDBClientTest {
    private static AbsNosqlClient nosqlClient = new MongoDBClient();

    @Test
    public void testCon() throws Exception {
        SourceDTO source = SourceDTO.builder()
                .url("kudu5:27107")
                .schema("admin")
                .username("admin")
                .password("123456")
                .build();

        Boolean isConnected = nosqlClient.testCon(source);
        if (!isConnected) {
            throw new DtCenterDefException("数据源连接异常");
        }
    }
}