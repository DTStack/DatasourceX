package com.dtstack.dtcenter.common.loader.postgresql;

import com.dtstack.dtcenter.common.loader.rdbms.AbsRdbmsClient;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.source.PostgresqlSourceDTO;
import org.junit.Test;

public class PostgresqlClientTest {

    private static AbsRdbmsClient rdbsClient = new PostgresqlClient();
    private PostgresqlSourceDTO source = PostgresqlSourceDTO.builder()
            .url("jdbc:postgresql://172.16.8.193:5432/DTstack?currentSchema=xq_libra")
            .username("root")
            .password("123456")
            .schema("")
            .build();

    private SqlQueryDTO sqlQueryDTO = SqlQueryDTO.builder().view(true).build();

    @Test
    public void test_issue() throws Exception {
        // 简单测试代码使用，具体全覆盖使用 core 包下面的
    }
}