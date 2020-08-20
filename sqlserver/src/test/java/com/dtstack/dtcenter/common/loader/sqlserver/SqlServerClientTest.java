package com.dtstack.dtcenter.common.loader.sqlserver;

import com.dtstack.dtcenter.common.loader.common.AbsRdbmsClient;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.source.SqlserverSourceDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import org.junit.Test;

import java.util.List;


public class SqlServerClientTest {
    private static AbsRdbmsClient rdbsClient = new SqlServerClient();
    private SqlserverSourceDTO source = SqlserverSourceDTO.builder()
            .url("jdbc:jtds:sqlserver://172.16.8.149:1433;DatabaseName=DTstack")
            .username("sa")
            .password("Dtstack2018")
            .schema("")
            .build();

    @Test
    public void testConnection() throws Exception {
        Boolean isConnected = rdbsClient.testCon(source);
        if (!isConnected) {
            throw new DtLoaderException("数据源连接异常");
        }
    }

    @Test
    public void getTableList() throws Exception {
        SqlQueryDTO sqlQueryDTO = SqlQueryDTO.builder().view(true).build();
        List tableList = rdbsClient.getTableList(source, sqlQueryDTO);
        System.out.println(tableList);


    }
}