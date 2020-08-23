package com.dtstack.dtcenter.common.loader.mongo;

import com.dtstack.dtcenter.loader.dto.source.MongoSourceDTO;
import org.junit.Test;

public class MongoDBClientTest {
    private static MongoDBClient nosqlClient = new MongoDBClient();

    private MongoSourceDTO source = MongoSourceDTO.builder()
            .hostPort("172.16.100.217:27017")
            .schema("admin")
            .username("root")
            .password("root")
            .build();

    @Test
    public void test_issue() throws Exception {
        // 简单测试代码使用，具体全覆盖使用 core 包下面的
    }
}