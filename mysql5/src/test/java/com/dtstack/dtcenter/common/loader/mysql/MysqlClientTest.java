package com.dtstack.dtcenter.common.loader.mysql;

import com.dtstack.dtcenter.common.exception.DtCenterDefException;
import com.dtstack.dtcenter.common.loader.common.AbsRdbmsClient;
import com.dtstack.dtcenter.loader.cache.cp.CpConfig;
import com.dtstack.dtcenter.loader.dto.source.Mysql5SourceDTO;
import org.junit.Test;

import java.sql.Connection;

public class MysqlClientTest {
    private static AbsRdbmsClient rdbsClient = new MysqlClient();

    Mysql5SourceDTO source = Mysql5SourceDTO.builder()
            .url("jdbc:mysql://172.16.101.249:3306/ide?useUnicode=true&characterEncoding=utf-8&tinyInt1isBit=false")
            .username("drpeco")
            .password("DT@Stack#123")
            .cpKey("233")
            .cpConfig(CpConfig.builder().build())
            .build();

    @Test
    public void getConnFactory() throws Exception {
        Boolean isConnected = rdbsClient.testCon(source);
        if (!isConnected) {
            throw new DtCenterDefException("数据源连接异常");
        }
    }

    @Test(expected = DtCenterDefException.class)
    public void getConnection () throws Exception {
        Connection con1 = rdbsClient.getCon(source);
        Connection con2 = rdbsClient.getCon(source);
        Connection con3 = rdbsClient.getCon(source);
        Connection con4 = rdbsClient.getCon(source);
        Connection con5 = rdbsClient.getCon(source);
        Connection con6 = rdbsClient.getCon(source);
        Connection con7 = rdbsClient.getCon(source);
        Connection con8 = rdbsClient.getCon(source);
        Connection con9 = rdbsClient.getCon(source);
        Connection con10 = rdbsClient.getCon(source);
        Connection con11 = rdbsClient.getCon(source);
    }
}