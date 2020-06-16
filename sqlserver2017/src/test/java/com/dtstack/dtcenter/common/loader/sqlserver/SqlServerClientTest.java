package com.dtstack.dtcenter.common.loader.sqlserver;

import com.dtstack.dtcenter.common.exception.DtCenterDefException;
import com.dtstack.dtcenter.common.loader.common.AbsRdbmsClient;
import com.dtstack.dtcenter.loader.dto.source.Sqlserver2017SourceDTO;
import org.junit.Test;


public class SqlServerClientTest {
    private static AbsRdbmsClient rdbsClient = new SqlServerClient();

    @Test
    public void testConnection() throws Exception {
        Sqlserver2017SourceDTO source = Sqlserver2017SourceDTO.builder()
                .url("jdbc:sqlserver://kudu5:1433;databaseName=tudou")
                .username("sa")
                .password("<root@Passw0rd>")
                .build();
        Boolean isConnected = rdbsClient.testCon(source);
        if (!isConnected) {
            throw new DtCenterDefException("数据源连接异常");
        }
    }
}