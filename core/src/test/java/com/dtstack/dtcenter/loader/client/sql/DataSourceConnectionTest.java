package com.dtstack.dtcenter.loader.client.sql;

import com.dtstack.dtcenter.common.enums.DataSourceType;
import com.dtstack.dtcenter.common.exception.DtCenterDefException;
import com.dtstack.dtcenter.loader.client.AbsClientCache;
import com.dtstack.dtcenter.loader.client.IClient;
import com.dtstack.dtcenter.loader.dto.SourceDTO;
import com.dtstack.dtcenter.loader.enums.ClientType;
import org.junit.Test;

public class DataSourceConnectionTest {

    private static final AbsClientCache clientCache = ClientType.DATA_SOURCE_CLIENT.getClientCache();

    @Test
    public void getMysqlClient() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.MySQL.getVal());
        SourceDTO source = SourceDTO.builder()
                .url("jdbc:mysql://172.16.8.109:3306/ide")
                .username("dtstack")
                .password("abc123")
                .build();
        Boolean isConnected = client.testCon(source);
        if (!isConnected) {
            throw new DtCenterDefException("数据源连接异常");
        }
    }

    @Test
    public void getOracleClient() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.Oracle.getVal());
        SourceDTO source = SourceDTO.builder()
                .url("jdbc:oracle:thin:@172.16.8.178:1521:xe")
                .username("dtstack")
                .password("abc123")
                .build();
        Boolean isConnected = client.testCon(source);
        if (!isConnected) {
            throw new DtCenterDefException("数据源连接异常");
        }
    }

    @Test
    public void getSqlServerClient() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.SQLServer.getVal());
        SourceDTO source = SourceDTO.builder()
                .url("jdbc:jtds:sqlserver://172.16.8.149:1433;DatabaseName=DTstack")
                .username("sa")
                .password("Dtstack2018")
                .build();
        Boolean isConnected = client.testCon(source);
        if (!isConnected) {
            throw new DtCenterDefException("数据源连接异常");
        }
    }

    @Test
    public void getPostgresqlClient() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.PostgreSQL.getVal());
        SourceDTO source = SourceDTO.builder()
                .url("jdbc:postgresql://172.16.8.193:5432/DTstack?currentSchema=public")
                .username("root")
                .password("abc123")
                .build();
        Boolean isConnected = client.testCon(source);
        if (!isConnected) {
            throw new DtCenterDefException("数据源连接异常");
        }
    }

    @Test
    public void getHiveClient() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.HIVE.getVal());
        SourceDTO source = SourceDTO.builder()
                .url("jdbc:hive2://cdh-impala2:10000")
                .username("root")
                .password("abc123")
                .build();
        Boolean isConnected = client.testCon(source);
        if (!isConnected) {
            throw new DtCenterDefException("数据源连接异常");
        }
    }

    @Test
    public void getSparkClient() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.HIVE1X.getVal());
        SourceDTO source = SourceDTO.builder()
                .url("jdbc:hive2://cdh-impala2:10000")
                .username("root")
                .password("abc123")
                .build();
        Boolean isConnected = client.testCon(source);
        if (!isConnected) {
            throw new DtCenterDefException("数据源连接异常");
        }
    }

    @Test
    public void getGbaseClient() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.GBase_8a.getVal());
        SourceDTO source = SourceDTO.builder()
                .url("jdbc:gbase://172.16.8.193:5258/dtstack")
                .username("root")
                .build();
        Boolean isConnected = client.testCon(source);
        if (!isConnected) {
            throw new DtCenterDefException("数据源连接异常");
        }
    }

    @Test
    public void getKylinClient() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.Kylin.getVal());
        SourceDTO source = SourceDTO.builder()
                .url("jdbc:kylin://172.16.100.105:7070/yctest")
                .username("ADMIN")
                .password("KYLIN")
                .build();
        Boolean isConnected = client.testCon(source);
        if (!isConnected) {
            throw new DtCenterDefException("数据源连接异常");
        }
    }

    @Test
    public void getImpalaClient() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.IMPALA.getVal());
        SourceDTO source = SourceDTO.builder()
                .url("jdbc:impala://cdh-impala1:21050;AuthMech=3")
                .username("root")
                .password("abc123")
                .build();
        Boolean isConnected = client.testCon(source);
        if (!isConnected) {
            throw new DtCenterDefException("数据源连接异常");
        }
    }
}