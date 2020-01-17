package com.dtstack.dtcenter.common.loader.rdbms.clickhouse;

import com.dtstack.dtcenter.common.exception.DtCenterDefException;
import com.dtstack.dtcenter.common.loader.rdbms.common.AbsRdbmsClient;
import com.dtstack.dtcenter.loader.dto.SourceDTO;
import org.junit.Test;

public class ClickhouseClientTest {
    private static AbsRdbmsClient rdbsClient = new ClickhouseClient();

    @Test
    public void getConnFactory() throws ClassNotFoundException {
        SourceDTO source = new SourceDTO.SourceDTOBuilder()
                .setUrl("jdbc:clickhouse://172.16.10.168:8123/mqTest")
                .setUsername("dtstack")
                .setPassword("abc123")
                .builder();
        Boolean isConnected = rdbsClient.testCon(source);
        if (!isConnected) {
            throw new DtCenterDefException("数据源连接异常");
        }
    }
}