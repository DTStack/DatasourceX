package com.dtstack.dtcenter.common.loader.kylin;

import com.dtstack.dtcenter.common.loader.common.AbsRdbmsClient;
import com.dtstack.dtcenter.loader.dto.source.KylinSourceDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import org.junit.Test;

public class KylinClientTest {
    private static AbsRdbmsClient rdbsClient = new KylinClient();

    @Test
    public void getConnFactory() throws Exception {
        KylinSourceDTO source = KylinSourceDTO.builder()
                .url("jdbc:kylin://172.16.100.105:7070/1005_9")
                .username("ADMIN")
                .password("KYLIN")
                .build();
        Boolean isConnected = rdbsClient.testCon(source);
        if (!isConnected) {
            throw new DtLoaderException("数据源连接异常");
        }
    }
}