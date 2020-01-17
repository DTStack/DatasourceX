package com.dtstack.dtcenter.common.loader.rdbms.gbase;

import com.dtstack.dtcenter.common.exception.DtCenterDefException;
import com.dtstack.dtcenter.common.loader.rdbms.common.AbsRdbmsClient;
import com.dtstack.dtcenter.loader.dto.SourceDTO;
import org.junit.Test;

public class GbaseClientTest {
    private static AbsRdbmsClient rdbsClient = new GbaseClient();

    @Test
    public void getConnFactory() throws ClassNotFoundException {
        SourceDTO source = new SourceDTO.SourceDTOBuilder()
                .setUrl("jdbc:gbase://172.16.8.193:5258/dtstack")
                .setUsername("root")
                .builder();
        Boolean isConnected = rdbsClient.testCon(source);
        if (!isConnected) {
            throw new DtCenterDefException("数据源连接异常");
        }
    }
}