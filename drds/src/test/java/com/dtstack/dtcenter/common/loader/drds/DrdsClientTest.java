package com.dtstack.dtcenter.common.loader.drds;

import com.dtstack.dtcenter.common.loader.common.AbsRdbmsClient;
import com.dtstack.dtcenter.loader.dto.source.RdbmsSourceDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import org.junit.Test;

public class DrdsClientTest {
    private static AbsRdbmsClient rdbsClient = new DrdsClient();

    @Test
    public void getConnFactory() throws Exception {
        RdbmsSourceDTO source = RdbmsSourceDTO.builder()
                .url("jdbc:mysql://172.16.8.109:3306/ide")
                .username("dtstack")
                .password("abc123")
                .build();
        Boolean isConnected = rdbsClient.testCon(source);
        if (!isConnected) {
            throw new DtLoaderException("数据源连接异常");
        }
    }
}