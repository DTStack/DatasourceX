package com.dtstack.dtcenter.common.loader.dm;

import com.dtstack.dtcenter.common.loader.rdbms.AbsRdbmsClient;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.source.DmSourceDTO;
import org.junit.Test;

/**
 * Date: 2020/4/19
 * Company: www.dtstack.com
 *
 * @author xiaochen
 */
public class DmClientTest {
    private static AbsRdbmsClient rdbsClient = new DmClient();
    DmSourceDTO source = DmSourceDTO.builder()
            .url("jdbc:dm://172.16.8.178:5236/chener")
            .username("chener")
            .password("abc123456")
            .schema("CHENSAN")
            .build();

    SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("TABLE_NAME").build();

    @Test
    public void test_issue() throws Exception {
        // 简单测试代码使用，具体全覆盖使用 core 包下面的
    }
}