package com.dtstack.dtcenter.common.loader.rdbms.kylin;

import com.dtstack.dtcenter.common.exception.DtCenterDefException;
import com.dtstack.dtcenter.common.loader.rdbms.common.AbsRdbmsClient;
import com.dtstack.dtcenter.loader.dto.SourceDTO;
import org.junit.Test;

public class KylinClientTest {
    private static AbsRdbmsClient rdbsClient = new KylinClient();

    @Test
    public void getConnFactory() throws ClassNotFoundException {
        SourceDTO source = new SourceDTO.SourceDTOBuilder()
                .setUrl("jdbc:kylin://172.16.100.105:7070/yctest")
                .setUsername("ADMIN")
                .setPassword("KYLIN")
                .builder();
        Boolean isConnected = rdbsClient.testCon(source);
        if (!isConnected) {
            throw new DtCenterDefException("数据源连接异常");
        }
    }
}