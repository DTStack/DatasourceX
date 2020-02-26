package com.dtstack.dtcenter.loader.client.sql;

import com.dtstack.dtcenter.common.enums.DataSourceType;
import com.dtstack.dtcenter.loader.client.AbsClientCache;
import com.dtstack.dtcenter.loader.client.IClient;
import com.dtstack.dtcenter.loader.dto.SourceDTO;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.enums.ClientType;
import org.junit.Test;

import java.util.List;
import java.util.Map;

public class DataSourceClientCacheBaseQueryTest {

    private static final AbsClientCache clientCache = ClientType.DATA_SOURCE_CLIENT.getClientCache();

    @Test
    public void getMysqlClient() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.MySQL.name());
        SourceDTO source = SourceDTO.builder()
                .url("jdbc:mysql://172.16.8.109:3306/ide")
                .username("dtstack")
                .password("abc123")
                .build();
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("show tables").build();
        List<Map<String, Object>> show_tables = client.executeQuery(source, queryDTO);
        System.out.println(show_tables.size());
    }

    @Test
    public void getOracleClient() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.Oracle.name());
        SourceDTO source = SourceDTO.builder()
                .url("jdbc:oracle:thin:@172.16.8.178:1521:xe")
                .username("dtstack")
                .password("abc123")
                .build();
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("SELECT * FROM all_tables").build();
        List<Map<String, Object>> show_tables = client.executeQuery(source, queryDTO);
        System.out.println(show_tables.size());
    }

    @Test
    public void getSqlServerClient() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.SQLServer.name());
        SourceDTO source = SourceDTO.builder()
                .url("jdbc:jtds:sqlserver://172.16.8.149:1433;DatabaseName=DTstack")
                .username("sa")
                .password("Dtstack2018")
                .build();
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("select * from sys.objects where type='U' or type='V'").build();
        List<Map<String, Object>> show_tables = client.executeQuery(source, queryDTO);
        System.out.println(show_tables.size());
    }

    @Test
    public void getPostgresqlClient() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.PostgreSQL.name());
        SourceDTO source = SourceDTO.builder()
                .url("jdbc:postgresql://172.16.8.193:5432/DTstack?currentSchema=public")
                .username("root")
                .password("abc123")
                .build();
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("select table_name from  information_schema.tables").build();
        List<Map<String, Object>> show_tables = client.executeQuery(source, queryDTO);
        System.out.println(show_tables.size());
    }

    @Test
    public void getClickhouseClient() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.Clickhouse.name());
        SourceDTO source = SourceDTO.builder()
                .url("jdbc:clickhouse://172.16.10.168:8123/mqTest")
                .username("dtstack")
                .password("abc123")
                .build();
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("show tables").build();
        List<Map<String, Object>> show_tables = client.executeQuery(source, queryDTO);
        System.out.println(show_tables.size());
    }

    @Test
    public void getHiveClient() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.HIVE1X.name());
        SourceDTO source = SourceDTO.builder()
                .url("jdbc:hive2://cdh-impala2:10000")
                .username("root")
                .password("abc123")
                .build();
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("show tables").build();
        List<Map<String, Object>> show_tables = client.executeQuery(source, queryDTO);
        System.out.println(show_tables.size());
    }

    @Test
    public void getSparkClient() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.HIVE.name());
        SourceDTO source = SourceDTO.builder()
                .url("jdbc:hive2://cdh-impala2:10000")
                .username("root")
                .password("abc123")
                .build();
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("show tables").build();
        List<Map<String, Object>> show_tables = client.executeQuery(source, queryDTO);
        System.out.println(show_tables.size());
    }

    @Test
    public void getGbaseClient() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.GBase_8a.name());
        SourceDTO source = SourceDTO.builder()
                .url("jdbc:gbase://172.16.8.193:5258/dtstack")
                .username("root")
                .build();
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("show tables").build();
        List<Map<String, Object>> show_tables = client.executeQuery(source, queryDTO);
        System.out.println(show_tables.size());
    }

    @Test
    public void getKylinClient() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.Kylin.name());
        SourceDTO source = SourceDTO.builder()
                .url("jdbc:kylin://172.16.100.105:7070/yctest")
                .username("ADMIN")
                .password("KYLIN")
                .build();
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("select 1111").build();
        List<Map<String, Object>> show_tables = client.executeQuery(source, queryDTO);
        System.out.println(show_tables.size());
    }

    @Test
    public void getImpalaClient() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.IMPALA.name());
        SourceDTO source = SourceDTO.builder()
                .url("jdbc:impala://cdh-impala1:21050;AuthMech=3")
                .username("root")
                .password("abc123")
                .build();
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("show tables").build();
        List<Map<String, Object>> show_tables = client.executeQuery(source, queryDTO);
        System.out.println(show_tables.size());
    }
}