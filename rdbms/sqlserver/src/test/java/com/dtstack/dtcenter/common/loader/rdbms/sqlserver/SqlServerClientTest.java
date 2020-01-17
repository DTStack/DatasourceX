package com.dtstack.dtcenter.common.loader.rdbms.sqlserver;

import com.dtstack.dtcenter.common.exception.DtCenterDefException;
import com.dtstack.dtcenter.common.loader.rdbms.common.AbsRdbmsClient;
import com.dtstack.dtcenter.loader.dto.SourceDTO;
import org.junit.Test;


public class SqlServerClientTest {
    private static AbsRdbmsClient rdbsClient = new SqlServerClient();

    @Test
    public void testConnection() throws ClassNotFoundException {
        SourceDTO source = new SourceDTO.SourceDTOBuilder()
                .setUrl("jdbc:jtds:sqlserver://172.16.8.149:1433;DatabaseName=DTstack")
                .setUsername("sa")
                .setPassword("Dtstack2018")
                .builder();
        Boolean isConnected = rdbsClient.testCon(source);
        if (!isConnected) {
            throw new DtCenterDefException("数据源连接异常");
        }
    }
}