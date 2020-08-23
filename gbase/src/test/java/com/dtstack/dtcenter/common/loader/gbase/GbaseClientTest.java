package com.dtstack.dtcenter.common.loader.gbase;

import com.dtstack.dtcenter.common.loader.rdbms.AbsRdbmsClient;
import com.dtstack.dtcenter.loader.dto.source.GBaseSourceDTO;
import org.junit.Test;

public class GbaseClientTest {
    private static AbsRdbmsClient rdbsClient = new GbaseClient();

    private GBaseSourceDTO source = GBaseSourceDTO.builder()
            .url("jdbc:gbase://172.16.8.193:5258/dtstack")
            .username("root")
            .password("123456")
            .build();

    @Test
    public void test_issue() throws Exception {
        // 简单测试代码使用，具体全覆盖使用 core 包下面的
    }
}