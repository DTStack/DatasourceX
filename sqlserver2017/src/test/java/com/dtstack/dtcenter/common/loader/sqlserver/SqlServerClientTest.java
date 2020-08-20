package com.dtstack.dtcenter.common.loader.sqlserver;

import com.dtstack.dtcenter.common.loader.common.AbsRdbmsClient;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.source.Sqlserver2017SourceDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import org.junit.Test;

import java.util.List;


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
            throw new DtLoaderException("数据源连接异常");
        }
    }

    @Test
    public void getAllDatabases() throws Exception {
        Sqlserver2017SourceDTO source = Sqlserver2017SourceDTO.builder()
                .url("jdbc:sqlserver://kudu5:1433;databaseName=tudou")
                .username("sa")
                .password("<root@Passw0rd>")
                .build();
        List<String> databases = rdbsClient.getAllDatabases(source, SqlQueryDTO.builder().build());
        System.out.println(databases);
    }


}