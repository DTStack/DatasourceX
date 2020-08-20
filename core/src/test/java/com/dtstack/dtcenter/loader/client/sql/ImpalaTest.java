package com.dtstack.dtcenter.loader.client.sql;

import com.dtstack.dtcenter.loader.cache.pool.config.PoolConfig;
import com.dtstack.dtcenter.loader.client.AbsClientCache;
import com.dtstack.dtcenter.loader.client.IClient;
import com.dtstack.dtcenter.loader.dto.ColumnMetaDTO;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.source.ImpalaSourceDTO;
import com.dtstack.dtcenter.loader.enums.ClientType;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import org.junit.Test;

import java.sql.Connection;
import java.util.List;
import java.util.Map;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 01:23 2020/2/29
 * @Description：Impala 测试
 */
public class ImpalaTest {
    private static final AbsClientCache clientCache = ClientType.DATA_SOURCE_CLIENT.getClientCache();

    ImpalaSourceDTO source = ImpalaSourceDTO.builder()
            .url("jdbc:impala://172.16.100.242:21050/default;AuthMech=3")
            .username("hxb")
            .password("admin123")
            .poolConfig(new PoolConfig())
            .build();

    @Test
    public void getCon() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.IMPALA.getPluginName());
        Connection con1 = client.getCon(source);
        String con1JdbcConn = con1.toString().split("wrapping")[1];
        Connection con2 = client.getCon(source);
        Connection con3 = client.getCon(source);
        Connection con4 = client.getCon(source);
        Connection con5 = client.getCon(source);
        Connection con6 = client.getCon(source);
        Connection con7 = client.getCon(source);
        Connection con8 = client.getCon(source);
        Connection con9 = client.getCon(source);
        Connection con10 = client.getCon(source);
        con1.close();
        Connection con11 = client.getCon(source);
        String con11JdbcConn = con11.toString().split("wrapping")[1];
        assert con1JdbcConn.equals(con11JdbcConn);
        con2.close();
        con3.close();
        con4.close();
        con5.close();
        con6.close();
        con7.close();
        con8.close();
        con9.close();
        con10.close();
        con11.close();
    }

    @Test
    public void testCon() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.IMPALA.getPluginName());
        Boolean isConnected = client.testCon(source);
        if (Boolean.FALSE.equals(isConnected)) {
            throw new DtLoaderException("连接异常");
        }
    }

    @Test
    public void executeQuery() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.IMPALA.getPluginName());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("show tables").build();
        List<Map<String, Object>> mapList = client.executeQuery(source, queryDTO);
        System.out.println(mapList.size());
    }

    @Test
    public void executeSqlWithoutResultSet() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.IMPALA.getPluginName());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("show tables").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
    }

    @Test
    public void getTableList() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.IMPALA.getPluginName());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().build();
        List<String> tableList = client.getTableList(source, queryDTO);
        System.out.println(tableList);
    }

    @Test
    public void getColumnClassInfo() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.IMPALA.getPluginName());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("ch02").build();
        List<String> columnClassInfo = client.getColumnClassInfo(source, queryDTO);
        System.out.println(columnClassInfo.size());
    }

    @Test
    public void getColumnMetaData() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.IMPALA.getPluginName());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("ch02").build();
        List<ColumnMetaDTO> columnMetaData = client.getColumnMetaData(source, queryDTO);
        System.out.println(columnMetaData.size());
    }

    @Test
    public void getTableMetaComment() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.IMPALA.getPluginName());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("ch02").build();
        String tableMetaComment = client.getTableMetaComment(source, queryDTO);
        System.out.println(tableMetaComment);
    }

    @Test
    public void getPreview() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.IMPALA.getPluginName());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().previewNum(2).tableName("ch02").build();
        List preview = client.getPreview(source, queryDTO);
        System.out.println(preview);
    }

    @Test
    public void getAllDatabases() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.IMPALA.getPluginName());
        System.out.println(client.getAllDatabases(source, SqlQueryDTO.builder().build()));
    }

    @Test
    public void getPartitionColumn() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.IMPALA.getPluginName());
        System.out.println(client.getPartitionColumn(source, SqlQueryDTO.builder().tableName("import").build()));
    }


    @Test
    public void getCreateSql() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.IMPALA.getPluginName());
        System.out.println(client.getCreateTableSql(source, SqlQueryDTO.builder().tableName("day").build()));
    }
}
