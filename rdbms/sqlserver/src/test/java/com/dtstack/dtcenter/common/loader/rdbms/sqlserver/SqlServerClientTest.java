package com.dtstack.dtcenter.common.loader.rdbms.sqlserver;

import com.dtstack.dtcenter.common.exception.DtCenterDefException;
import com.dtstack.dtcenter.common.loader.rdbms.common.AbsRdbmsClient;
import com.dtstack.dtcenter.loader.dto.SourceDTO;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import org.junit.Test;

import java.util.List;


public class SqlServerClientTest {
    private static AbsRdbmsClient rdbsClient = new SqlServerClient();
    private SourceDTO source = SourceDTO.builder()
            .url("jdbc:jtds:sqlserver://172.16.8.149:1433;DatabaseName=DTstack")
            .username("sa")
            .password("Dtstack2018")
            .schema("")
            .build();

    @Test
    public void testConnection() throws Exception {
        Boolean isConnected = rdbsClient.testCon(source);
        if (!isConnected) {
            throw new DtCenterDefException("数据源连接异常");
        }
    }

    @Test
    public void getTableList() throws Exception {
        SqlQueryDTO sqlQueryDTO = SqlQueryDTO.builder().view(true).build();
        List tableList = rdbsClient.getTableList(source, sqlQueryDTO);
        System.out.println(tableList);


    }
}