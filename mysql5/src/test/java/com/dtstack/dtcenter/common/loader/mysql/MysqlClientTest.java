package com.dtstack.dtcenter.common.loader.mysql;

import com.dtstack.dtcenter.common.loader.rdbms.AbsRdbmsClient;
import com.dtstack.dtcenter.loader.dto.source.Mysql5SourceDTO;
import org.junit.Test;

public class MysqlClientTest {
    private static AbsRdbmsClient rdbsClient = new MysqlClient();

    Mysql5SourceDTO source = Mysql5SourceDTO.builder()
            .url("jdbc:mysql://172.16.101.249:3306/ide?useUnicode=true&characterEncoding=utf-8&tinyInt1isBit=false")
            .username("drpeco")
            .password("DT@Stack#123")
            //.poolConfig(PoolConfig.builder().build())
            .build();

    @Test
    public void test_issue() throws Exception {
        // 简单测试代码使用，具体全覆盖使用 core 包下面的
    }
}