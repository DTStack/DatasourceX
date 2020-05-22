package com.dtstack.dtcenter.common.loader.mysql;

import com.dtstack.dtcenter.common.exception.DtCenterDefException;
import com.dtstack.dtcenter.common.loader.common.AbsRdbmsClient;
import com.dtstack.dtcenter.loader.dto.SourceDTO;
import org.junit.Test;

public class MysqlClientTest {
    private static AbsRdbmsClient rdbsClient = new MysqlClient();

    @Test
    public void getConnFactory() throws Exception {
        SourceDTO source = SourceDTO.builder()
                .url("jdbc:mysql://172.16.8.109:3306/ide")
                .username("dtstack")
                .password("abc123")
                .build();
        Boolean isConnected = rdbsClient.testCon(source);
        if (!isConnected) {
            throw new DtCenterDefException("数据源连接异常");
        }
    }
}