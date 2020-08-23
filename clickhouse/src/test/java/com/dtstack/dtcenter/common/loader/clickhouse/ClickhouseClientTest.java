package com.dtstack.dtcenter.common.loader.clickhouse;

import com.dtstack.dtcenter.common.loader.rdbms.AbsRdbmsClient;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.source.ClickHouseSourceDTO;
import org.junit.Test;

public class ClickhouseClientTest {
    private static AbsRdbmsClient rdbsClient = new ClickhouseClient();

    ClickHouseSourceDTO source = ClickHouseSourceDTO.builder()
            .url("jdbc:clickhouse://172.16.10.168:8123/mqTest")
            .username("dtstack")
            .password("abc123")
            .schema("mqTest")
            .build();
    SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("cust").build();

    @Test
    public void test_issue() throws Exception {
        // 简单测试代码使用，具体全覆盖使用 core 包下面的
    }
}