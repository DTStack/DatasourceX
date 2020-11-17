package com.dtstack.dtcenter.loader.client.sql;

import com.dtstack.dtcenter.common.exception.DtCenterDefException;
import com.dtstack.dtcenter.loader.IDownloader;
import com.dtstack.dtcenter.loader.cache.pool.config.PoolConfig;
import com.dtstack.dtcenter.loader.client.AbsClientCache;
import com.dtstack.dtcenter.loader.client.IClient;
import com.dtstack.dtcenter.loader.dto.ColumnMetaDTO;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.source.PostgresqlSourceDTO;
import com.dtstack.dtcenter.loader.enums.ClientType;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import org.apache.commons.collections.CollectionUtils;
import org.junit.Assert;
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
            .url("jdbc:postgresql://172.16.8.193:5432/database")
            .username("root")
            .password("postgresql")
            //.schema("pg_catalog")
            .poolConfig(new PoolConfig())
            .build();

    @Test
    public void getCon() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.PostgreSQL.getPluginName());
        Connection con1 = client.getCon(source);
        String con1JdbcConn = con1.toString().split("PgConnection")[1];
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
        String con11JdbcConn = con11.toString().split("PgConnection")[1];
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
        IClient client = clientCache.getClient(DataSourceType.PostgreSQL.getPluginName());
        Boolean isConnected = client.testCon(source);
        if (Boolean.FALSE.equals(isConnected)) {
            throw new DtCenterDefException("连接异常");
        }
    }

    @Test
    public void executeQuery() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.PostgreSQL.getPluginName());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("select 1111").build();
        List<Map<String, Object>> mapList = client.executeQuery(source, queryDTO);
        System.out.println(mapList.size());
    }

    @Test
    public void executeSqlWithoutResultSet() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.PostgreSQL.getPluginName());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("select 1111").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
    }

    /**
     * 获取表测试：没有schema，包括视图
     * @throws Exception
     */
    @Test
    public void getTableListNoSchemaView() throws Exception {
        source.setSchema(null);
        IClient client = clientCache.getClient(DataSourceType.PostgreSQL.getPluginName());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().view(true).build();
        List<String> tableList = client.getTableList(source, queryDTO);
        Assert.assertTrue(CollectionUtils.isNotEmpty(tableList));
    }

    /**
     * 获取表测试：没有schema，不包括视图
     * @throws Exception
     */
    @Test
    public void getTableListNoSchemaNoView() throws Exception {
        source.setSchema(null);
        IClient client = clientCache.getClient(DataSourceType.PostgreSQL.getPluginName());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().view(false).build();
        List<String> tableList = client.getTableList(source, queryDTO);
        Assert.assertTrue(CollectionUtils.isNotEmpty(tableList));
    }

    /**
     * 获取表测试：有schema，包括视图
     * @throws Exception
     */
    @Test
    public void getTableListSchemaView() throws Exception {
        source.setSchema("pg_catalog");
        IClient client = clientCache.getClient(DataSourceType.PostgreSQL.getPluginName());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().view(true).build();
        List<String> tableList = client.getTableList(source, queryDTO);
        Assert.assertTrue(CollectionUtils.isNotEmpty(tableList));
    }

    /**
     * 获取表测试：有schema，不包括视图
     * @throws Exception
     */
    @Test
    public void getTableListSchemaNoView() throws Exception {
        source.setSchema("pg_catalog");
        IClient client = clientCache.getClient(DataSourceType.PostgreSQL.getPluginName());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().view(false).build();
        List<String> tableList = client.getTableList(source, queryDTO);
        Assert.assertTrue(CollectionUtils.isNotEmpty(tableList));
    }

    @Test
    public void getColumnClassInfo() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.PostgreSQL.getPluginName());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("table_test").build();
        List<String> columnClassInfo = client.getColumnClassInfo(source, queryDTO);
        System.out.println(columnClassInfo.size());
    }

    @Test
    public void getColumnMetaData() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.PostgreSQL.getPluginName());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("table_test").build();
        List<ColumnMetaDTO> columnMetaData = client.getColumnMetaData(source, queryDTO);
        System.out.println(columnMetaData.size());
    }

    @Test
    public void getColumnMetaDataBySchema() throws Exception {
        source.setSchema("yunchuan");
        IClient client = clientCache.getClient(DataSourceType.PostgreSQL.getPluginName());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("test").build();
        List<ColumnMetaDTO> columnMetaData = client.getColumnMetaData(source, queryDTO);
        Assert.assertTrue(CollectionUtils.isNotEmpty(columnMetaData));
    }

    @Test
    public void getTableMetaComment() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.PostgreSQL.getPluginName());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("table_test").build();
        String metaComment = client.getTableMetaComment(source, queryDTO);
        System.out.println(metaComment);
    }

    @Test
    public void getDownloader() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.PostgreSQL.getPluginName());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("select * from table_test").build();
        IDownloader downloader = client.getDownloader(source, queryDTO);
        int i = 0;
        while (!downloader.reachedEnd()){
            List<List<String>> o = (List<List<String>>)downloader.readNext();
            System.out.println("========================"+i+"========================");
            for (List list:o){
                System.out.println(list);
            }
            i++;
        }
    }

    @Test
    public void getPreview() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.PostgreSQL.getPluginName());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("table_test").previewNum(6).build();
        List preview = client.getPreview(source, queryDTO);
        System.out.println(preview);
    }

    /**
     * 根据schema + tableName 数据预览
     * @throws Exception
     */
    @Test
    public void getPreviewBySchema() throws Exception {
        source.setSchema("yunchuan");
        IClient client = clientCache.getClient(DataSourceType.PostgreSQL.getPluginName());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("test").previewNum(3).build();
        List preview = client.getPreview(source, queryDTO);
        Assert.assertTrue(CollectionUtils.isNotEmpty(preview));
    }

    @Test
    public void getAllDatabases() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.PostgreSQL.getPluginName());
        List<String> databases = client.getAllDatabases(source, SqlQueryDTO.builder().build());
        System.out.println(databases);
    }
}
