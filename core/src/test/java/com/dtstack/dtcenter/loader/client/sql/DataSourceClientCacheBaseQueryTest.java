package com.dtstack.dtcenter.loader.client.sql;

import com.dtstack.dtcenter.common.enums.DataSourceType;
import com.dtstack.dtcenter.loader.client.AbsClientCache;
import com.dtstack.dtcenter.loader.client.IClient;
import com.dtstack.dtcenter.loader.dto.SourceDTO;
import com.dtstack.dtcenter.loader.enums.ClientType;
import org.junit.Test;

import java.util.List;
import java.util.Map;

public class DataSourceClientCacheBaseQueryTest {

    private static final AbsClientCache clientCache = ClientType.DATA_SOURCE_CLIENT.getClientCache();

    @Test
    public void getMysqlClient() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.MySQL.name());
        SourceDTO source = new SourceDTO.SourceDTOBuilder()
                .setUrl("jdbc:mysql://172.16.8.109:3306/ide")
                .setUsername("dtstack")
                .setPassword("abc123")
                .builder();
        List<Map<String, Object>> show_tables = client.executeQuery(source, "show tables");
        System.out.println(show_tables.size());
    }

    @Test
    public void getOracleClient() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.Oracle.name());
        SourceDTO source = new SourceDTO.SourceDTOBuilder()
                .setUrl("jdbc:oracle:thin:@172.16.8.178:1521:xe")
                .setUsername("dtstack")
                .setPassword("abc123")
                .builder();
        List<Map<String, Object>> show_tables = client.executeQuery(source, "SELECT * FROM all_tables");
        System.out.println(show_tables.size());
    }

    @Test
    public void getSqlServerClient() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.SQLServer.name());
        SourceDTO source = new SourceDTO.SourceDTOBuilder()
                .setUrl("jdbc:jtds:sqlserver://172.16.8.149:1433;DatabaseName=DTstack")
                .setUsername("sa")
                .setPassword("Dtstack2018")
                .builder();
        List<Map<String, Object>> show_tables = client.executeQuery(source, "select * from sys.objects where type='U'" +
                " or" +
                " type='V'");
        System.out.println(show_tables.size());
    }

    @Test
    public void getPostgresqlClient() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.PostgreSQL.name());
        SourceDTO source = new SourceDTO.SourceDTOBuilder()
                .setUrl("jdbc:postgresql://172.16.8.193:5432/DTstack?currentSchema=public")
                .setUsername("root")
                .setPassword("abc123")
                .builder();
        List<Map<String, Object>> show_tables = client.executeQuery(source, "select table_name from " +
                "information_schema" +
                ".tables");
        System.out.println(show_tables.size());
    }

    @Test
    public void getClickhouseClient() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.Clickhouse.name());
        SourceDTO source = new SourceDTO.SourceDTOBuilder()
                .setUrl("jdbc:clickhouse://172.16.10.168:8123/mqTest")
                .setUsername("dtstack")
                .setPassword("abc123")
                .builder();
        List<Map<String, Object>> show_tables = client.executeQuery(source, "show tables");
        System.out.println(show_tables.size());
    }

    @Test
    public void getHiveClient() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.HIVE1X.name());
        SourceDTO source = new SourceDTO.SourceDTOBuilder()
                .setUrl("jdbc:hive2://cdh-impala2:10000")
                .setUsername("root")
                .setPassword("abc123")
                .builder();
        List<Map<String, Object>> show_tables = client.executeQuery(source, "show tables");
        System.out.println(show_tables.size());
    }

    @Test
    public void getSparkClient() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.HIVE.name());
        SourceDTO source = new SourceDTO.SourceDTOBuilder()
                .setUrl("jdbc:hive2://cdh-impala2:10000")
                .setUsername("root")
                .setPassword("abc123")
                .builder();
        List<Map<String, Object>> show_tables = client.executeQuery(source, "show tables");
        System.out.println(show_tables.size());
    }

    @Test
    public void getGbaseClient() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.GBase_8a.name());
        SourceDTO source = new SourceDTO.SourceDTOBuilder()
                .setUrl("jdbc:gbase://172.16.8.193:5258/dtstack")
                .setUsername("root")
                .builder();
        List<Map<String, Object>> show_tables = client.executeQuery(source, "show tables");
        System.out.println(show_tables.size());
    }

    @Test
    public void getKylinClient() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.Kylin.name());
        SourceDTO source = new SourceDTO.SourceDTOBuilder()
                .setUrl("jdbc:kylin://172.16.100.105:7070/yctest")
                .setUsername("ADMIN")
                .setPassword("KYLIN")
                .builder();
        List<Map<String, Object>> show_tables = client.executeQuery(source, "select 1111");
        System.out.println(show_tables.size());
    }

    @Test
    public void getImpalaClient() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.IMPALA.name());
        SourceDTO source = new SourceDTO.SourceDTOBuilder()
                .setUrl("jdbc:impala://cdh-impala1:21050;AuthMech=3")
                .setUsername("root")
                .setPassword("abc123")
                .builder();
        List<Map<String, Object>> show_tables = client.executeQuery(source, "show tables");
        System.out.println(show_tables.size());
    }
}