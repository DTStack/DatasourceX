package com.dtstack.dtcenter.common.loader.rdbms.kylin;

import com.dtstack.dtcenter.common.exception.DtCenterDefException;
import com.dtstack.dtcenter.common.loader.rdbms.common.AbsRdbmsClient;
import com.dtstack.dtcenter.loader.dto.SourceDTO;
import org.junit.Test;

public class KylinClientTest {
    private static AbsRdbmsClient rdbsClient = new KylinClient();

    @Test
    public void getConnFactory() throws Exception {
        SourceDTO source = SourceDTO.builder()
                .url("jdbc:kylin://172.16.100.105:7070/yctest")
                .username("ADMIN")
                .password("KYLIN")
                .build();
        Boolean isConnected = rdbsClient.testCon(source);
        if (!isConnected) {
            throw new DtCenterDefException("数据源连接异常");
        }
    }
}