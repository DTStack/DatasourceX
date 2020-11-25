package com.dtstack.dtcenter.loader.client.sql;

import com.dtstack.dtcenter.loader.IDownloader;
import com.dtstack.dtcenter.loader.cache.pool.config.PoolConfig;
import com.dtstack.dtcenter.loader.client.ClientCache;
import com.dtstack.dtcenter.loader.client.IClient;
import com.dtstack.dtcenter.loader.dto.ColumnMetaDTO;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.source.PostgresqlSourceDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import org.junit.Assert;
import org.junit.BeforeClass;
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
    private static PostgresqlSourceDTO source = PostgresqlSourceDTO.builder()
            .url("jdbc:postgresql://kudu5:54321/database?currentSchema=public")
            .username("postgres")
            .password("password")
            .poolConfig(new PoolConfig())
            .build();

    @BeforeClass
    public static void beforeClass() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.PostgreSQL.getVal());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("drop table if exists \"public\".nanqi").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("create table \"public\".nanqi (id int, name text)").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("insert into \"public\".nanqi values (1, 'nanqi')").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
    }

    @Test
    public void getCon() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.PostgreSQL.getVal());
        Connection con1 = client.getCon(source);
        con1.close();
    }

    @Test
    public void testCon() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.PostgreSQL.getVal());
        Boolean isConnected = client.testCon(source);
        if (Boolean.FALSE.equals(isConnected)) {
            throw new DtLoaderException("连接异常");
        }
    }

    @Test
    public void executeQuery() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.PostgreSQL.getVal());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("select 1111").build();
        List<Map<String, Object>> mapList = client.executeQuery(source, queryDTO);
        System.out.println(mapList.size());
    }

    @Test
    public void executeSqlWithoutResultSet() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.PostgreSQL.getVal());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("select 1111").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
    }

    /**
     * 获取表测试：没有schema，包括视图
     * @throws Exception
     */
    @Test
    public void getTableList() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.PostgreSQL.getVal());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().build();
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
        IClient client = ClientCache.getClient(DataSourceType.PostgreSQL.getPluginName());
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
        IClient client = ClientCache.getClient(DataSourceType.PostgreSQL.getPluginName());
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
        IClient client = ClientCache.getClient(DataSourceType.PostgreSQL.getPluginName());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().view(false).build();
        List<String> tableList = client.getTableList(source, queryDTO);
        Assert.assertTrue(CollectionUtils.isNotEmpty(tableList));
    }

    @Test
    public void getTableListBySchema() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.PostgreSQL.getVal());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().schema("pg_catalog").build();
        List<String> tableList = client.getTableListBySchema(source, queryDTO);
        System.out.println(tableList);
    }

    @Test
    public void getColumnClassInfo() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.PostgreSQL.getVal());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("nanqi").build();
        List<String> columnClassInfo = client.getColumnClassInfo(source, queryDTO);
        System.out.println(columnClassInfo.size());
    }

    @Test
    public void getColumnMetaData() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.PostgreSQL.getVal());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("nanqi").build();
        List<ColumnMetaDTO> columnMetaData = client.getColumnMetaData(source, queryDTO);
        System.out.println(columnMetaData.size());
    }

    @Test
    public void getColumnMetaDataBySchema() throws Exception {
        source.setSchema("yunchuan");
        IClient client = ClientCache.getClient(DataSourceType.PostgreSQL.getPluginName());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("test").build();
        List<ColumnMetaDTO> columnMetaData = client.getColumnMetaData(source, queryDTO);
        Assert.assertTrue(CollectionUtils.isNotEmpty(columnMetaData));
    }

    @Test
    public void getTableMetaComment() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.PostgreSQL.getVal());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("nanqi").build();
        String metaComment = client.getTableMetaComment(source, queryDTO);
        System.out.println(metaComment);
    }

    @Test
    public void getDownloader() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.PostgreSQL.getVal());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("select * from nanqi").build();
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
        IClient client = ClientCache.getClient(DataSourceType.PostgreSQL.getVal());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("nanqi").previewNum(6).build();
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
        IClient client = ClientCache.getClient(DataSourceType.PostgreSQL.getPluginName());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("test").previewNum(3).build();
        List preview = client.getPreview(source, queryDTO);
        Assert.assertTrue(CollectionUtils.isNotEmpty(preview));
    }

    @Test
    public void getAllDatabases() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.PostgreSQL.getVal());
        List<String> databases = client.getAllDatabases(source, SqlQueryDTO.builder().build());
        System.out.println(databases);
    }

    @Test
    public void getCurrentDatabase() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.PostgreSQL.getVal());
        String currentDatabase = client.getCurrentDatabase(source);
        Assert.assertNotNull(currentDatabase);
    }
}
