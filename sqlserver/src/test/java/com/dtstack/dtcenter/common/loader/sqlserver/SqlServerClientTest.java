package com.dtstack.dtcenter.common.loader.sqlserver;

import com.dtstack.dtcenter.common.loader.rdbms.AbsRdbmsClient;
import com.dtstack.dtcenter.loader.dto.source.SqlserverSourceDTO;
import org.junit.Test;


public class SqlServerClientTest {
    private static AbsRdbmsClient rdbsClient = new SqlServerClient();
    private SqlserverSourceDTO source = SqlserverSourceDTO.builder()
            .url("jdbc:jtds:sqlserver://172.16.8.149:1433;DatabaseName=DTstack")
            .username("sa")
            .password("Dtstack2018")
            .schema("")
            .build();

    @Test
    public void test_issue() throws Exception {
        // 简单测试代码使用，具体全覆盖使用 core 包下面的
    }
}