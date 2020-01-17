package com.dtstack.dtcenter.loader.client.sql;

import com.dtstack.dtcenter.common.enums.DataSourceType;
import com.dtstack.dtcenter.common.exception.DtCenterDefException;
import com.dtstack.dtcenter.loader.client.AbsClientCache;
import com.dtstack.dtcenter.loader.client.IClient;
import com.dtstack.dtcenter.loader.dto.SourceDTO;
import com.dtstack.dtcenter.loader.enums.ClientType;
import org.junit.Test;

public class DataSourceClientCacheTest {

    private static final AbsClientCache clientCache = ClientType.DATA_SOURCE_CLIENT.getClientCache();

    @Test
    public void getMysqlClient() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.MySQL.name());
        SourceDTO source = new SourceDTO.SourceDTOBuilder()
                .setUrl("jdbc:mysql://172.16.8.109:3306/ide")
                .setUsername("dtstack")
                .setPassword("abc123")
                .builder();
        Boolean isConnected = client.testCon(source);
        if (!isConnected) {
            throw new DtCenterDefException("数据源连接异常");
        }
    }

    @Test
    public void getOracleClient() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.Oracle.name());
        SourceDTO source = new SourceDTO.SourceDTOBuilder()
                .setUrl("jdbc:oracle:thin:@172.16.8.178:1521:xe")
                .setUsername("dtstack")
                .setPassword("abc123")
                .builder();
        Boolean isConnected = client.testCon(source);
        if (!isConnected) {
            throw new DtCenterDefException("数据源连接异常");
        }
    }

    @Test
    public void getSqlServerClient() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.SQLServer.name());
        SourceDTO source = new SourceDTO.SourceDTOBuilder()
                .setUrl("jdbc:jtds:sqlserver://172.16.8.149:1433;DatabaseName=DTstack")
                .setUsername("sa")
                .setPassword("Dtstack2018")
                .builder();
        Boolean isConnected = client.testCon(source);
        if (!isConnected) {
            throw new DtCenterDefException("数据源连接异常");
        }
    }

    @Test
    public void getPostgresqlClient() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.PostgreSQL.name());
        SourceDTO source = new SourceDTO.SourceDTOBuilder()
                .setUrl("jdbc:postgresql://172.16.8.193:5432/DTstack?currentSchema=public")
                .setUsername("root")
                .setPassword("abc123")
                .builder();
        Boolean isConnected = client.testCon(source);
        if (!isConnected) {
            throw new DtCenterDefException("数据源连接异常");
        }
    }

    @Test
    public void getClickhouseClient() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.Clickhouse.name());
        SourceDTO source = new SourceDTO.SourceDTOBuilder()
                .setUrl("jdbc:clickhouse://172.16.10.168:8123/mqTest")
                .setUsername("dtstack")
                .setPassword("abc123")
                .builder();
        Boolean isConnected = client.testCon(source);
        if (!isConnected) {
            throw new DtCenterDefException("数据源连接异常");
        }
    }

    @Test
    public void getHiveClient() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.HIVE.name());
        SourceDTO source = new SourceDTO.SourceDTOBuilder()
                .setUrl("jdbc:hive2://cdh-impala2:10000")
                .setUsername("root")
                .setPassword("abc123")
                .builder();
        Boolean isConnected = client.testCon(source);
        if (!isConnected) {
            throw new DtCenterDefException("数据源连接异常");
        }
    }

    @Test
    public void getSparkClient() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.HIVE1X.name());
        SourceDTO source = new SourceDTO.SourceDTOBuilder()
                .setUrl("jdbc:hive2://cdh-impala2:10000")
                .setUsername("root")
                .setPassword("abc123")
                .builder();
        Boolean isConnected = client.testCon(source);
        if (!isConnected) {
            throw new DtCenterDefException("数据源连接异常");
        }
    }

    @Test
    public void getGbaseClient() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.GBase_8a.name());
        SourceDTO source = new SourceDTO.SourceDTOBuilder()
                .setUrl("jdbc:gbase://172.16.8.193:5258/dtstack")
                .setUsername("root")
                .builder();
        Boolean isConnected = client.testCon(source);
        if (!isConnected) {
            throw new DtCenterDefException("数据源连接异常");
        }
    }

    @Test
    public void getKylinClient() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.Kylin.name());
        SourceDTO source = new SourceDTO.SourceDTOBuilder()
                .setUrl("jdbc:kylin://172.16.100.105:7070/yctest")
                .setUsername("ADMIN")
                .setPassword("KYLIN")
                .builder();
        Boolean isConnected = client.testCon(source);
        if (!isConnected) {
            throw new DtCenterDefException("数据源连接异常");
        }
    }

    @Test
    public void getImpalaClient() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.IMPALA.name());
        SourceDTO source = new SourceDTO.SourceDTOBuilder()
                .setUrl("jdbc:impala://cdh-impala1:21050;AuthMech=3")
                .setUsername("root")
                .setPassword("abc123")
                .builder();
        Boolean isConnected = client.testCon(source);
        if (!isConnected) {
            throw new DtCenterDefException("数据源连接异常");
        }
    }
}