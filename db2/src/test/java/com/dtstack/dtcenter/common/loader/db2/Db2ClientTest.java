package com.dtstack.dtcenter.common.loader.db2;

import com.dtstack.dtcenter.common.loader.rdbms.AbsRdbmsClient;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.source.Db2SourceDTO;
import org.junit.Test;

public class Db2ClientTest {
    private static AbsRdbmsClient rdbsClient = new Db2Client();
    Db2SourceDTO source = Db2SourceDTO.builder()
            .url("jdbc:db2://kudu5:50000/xiaochen")
            .username("xiaochen")
            .password("Abc1234")
            .build();

    SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("EMPLOYEE").build();

    @Test
    public void test_issue() throws Exception {
        // 简单测试代码使用，具体全覆盖使用 core 包下面的
    }
}