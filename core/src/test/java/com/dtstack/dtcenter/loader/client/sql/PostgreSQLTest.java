package com.dtstack.dtcenter.loader.client.sql;

import com.dtstack.dtcenter.common.enums.DataSourceClientType;
import com.dtstack.dtcenter.common.exception.DtCenterDefException;
import com.dtstack.dtcenter.loader.client.AbsClientCache;
import com.dtstack.dtcenter.loader.client.IClient;
import com.dtstack.dtcenter.loader.dto.ColumnMetaDTO;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.source.PostgresqlSourceDTO;
import com.dtstack.dtcenter.loader.enums.ClientType;
import org.junit.Test;

import java.sql.Connection;
import java.util.List;
import java.util.Map;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 03:53 2020/2/29
 * @Description：PostgreSQL 测试
 */
public class PostgreSQLTest {
    private static final AbsClientCache clientCache = ClientType.DATA_SOURCE_CLIENT.getClientCache();

    PostgresqlSourceDTO source = PostgresqlSourceDTO.builder()
            .url("jdbc:postgresql://172.16.8.193:5432/DTstack?currentSchema=public")
            .username("root")
            .password("abc123")
            .build();

    @Test
    public void getCon() throws Exception {
        IClient client = clientCache.getClient(DataSourceClientType.PostgreSQL.getPluginName());
        Connection con = client.getCon(source);
        con.createStatement().close();
        con.close();
    }

    @Test
    public void testCon() throws Exception {
        IClient client = clientCache.getClient(DataSourceClientType.PostgreSQL.getPluginName());
        Boolean isConnected = client.testCon(source);
        if (Boolean.FALSE.equals(isConnected)) {
            throw new DtCenterDefException("连接异常");
        }
    }

    @Test
    public void executeQuery() throws Exception {
        IClient client = clientCache.getClient(DataSourceClientType.PostgreSQL.getPluginName());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("select 1111").build();
        List<Map<String, Object>> mapList = client.executeQuery(source, queryDTO);
        System.out.println(mapList.size());
    }

    @Test
    public void executeSqlWithoutResultSet() throws Exception {
        IClient client = clientCache.getClient(DataSourceClientType.PostgreSQL.getPluginName());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("select 1111").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
    }

    @Test
    public void getTableList() throws Exception {
        IClient client = clientCache.getClient(DataSourceClientType.PostgreSQL.getPluginName());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().build();
        List<String> tableList = client.getTableList(source, queryDTO);
        System.out.println(tableList.size());
    }

    @Test
    public void getColumnClassInfo() throws Exception {
        IClient client = clientCache.getClient(DataSourceClientType.PostgreSQL.getPluginName());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("employee").build();
        List<String> columnClassInfo = client.getColumnClassInfo(source, queryDTO);
        System.out.println(columnClassInfo.size());
    }

    @Test
    public void getColumnMetaData() throws Exception {
        IClient client = clientCache.getClient(DataSourceClientType.PostgreSQL.getPluginName());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("employee").build();
        List<ColumnMetaDTO> columnMetaData = client.getColumnMetaData(source, queryDTO);
        System.out.println(columnMetaData.size());
    }

    @Test
    public void getTableMetaComment() throws Exception {
        IClient client = clientCache.getClient(DataSourceClientType.PostgreSQL.getPluginName());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("employee").build();
        String metaComment = client.getTableMetaComment(source, queryDTO);
        System.out.println(metaComment);
    }
}
