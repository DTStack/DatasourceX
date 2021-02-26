package com.dtstack.dtcenter.loader.client.sql;

import com.dtstack.dtcenter.loader.cache.pool.config.PoolConfig;
import com.dtstack.dtcenter.loader.client.ClientCache;
import com.dtstack.dtcenter.loader.client.IClient;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.source.VerticaSourceDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.sql.Connection;
import java.util.List;
import java.util.Map;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 14:31 2020/12/10
 * @Description：Vertica 测试
 */
@Ignore
public class VerticaTest {

    // 获取数据源 client
    private static final IClient client = ClientCache.getClient(DataSourceType.VERTICA.getVal());

    // 构建数据源信息 ps：em没有对应的数据源信息
    private static final VerticaSourceDTO source = VerticaSourceDTO.builder()
            .url("jdbc:vertica://172.16.8.178:5433/docker")
            .username("dbadmin")
            .poolConfig(PoolConfig.builder().build())
            .build();

    /**
     * 数据预处理
     */
    @BeforeClass
    public static void beforeClass() {
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("drop table if exists LOADER_TEST").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("create table LOADER_TEST (id int, name varchar(50))").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("comment on table LOADER_TEST is 'table comment'").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("insert into LOADER_TEST values (1, 'LOADER_TEST')").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
    }

    /**
     * 获取连接测试
     */
    @Test
    public void getCon() throws Exception {
        Connection connection = client.getCon(source);
        Assert.assertNotNull(connection);
    }

    /**
     * 连通性测试
     */
    @Test
    public void testCon() throws Exception {
        Boolean isConnected = client.testCon(source);
        Assert.assertTrue(isConnected);
    }

    /**
     * 测试连通性测试失败测试
     */
    @Test(expected = DtLoaderException.class)
    public void testErrorCon(){
        // 构建数据源信息
        VerticaSourceDTO source = VerticaSourceDTO.builder()
                .url("jdbc:vertica://172.16.101.225:5433/docker")
                .username("nanqi")
                .poolConfig(PoolConfig.builder().build())
                .build();
        client.testCon(source);
    }

    /**
     * 简单查询
     */
    @Test
    public void executeQuery() {
        IClient client = ClientCache.getClient(DataSourceType.VERTICA.getVal());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("select * from LOADER_TEST").build();
        List<Map<String, Object>> mapList = client.executeQuery(source, queryDTO);
        Assert.assertTrue(CollectionUtils.isNotEmpty(mapList));
    }

    /**
     * 获取表列表
     */
    @Test
    public void getTableList() {
        IClient client = ClientCache.getClient(DataSourceType.VERTICA.getVal());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().build();
        List<String> tableList = client.getTableList(source, queryDTO);
        Assert.assertTrue(CollectionUtils.isNotEmpty(tableList));
    }

    /**
     * 获取表注释
     */
    @Test
    public void getTableMetaComment() {
        IClient client = ClientCache.getClient(DataSourceType.VERTICA.getVal());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("nanqi.\"ide.nanqi\"").build();
        String metaComment = client.getTableMetaComment(source, queryDTO);
        Assert.assertTrue(StringUtils.isNotBlank(metaComment));
    }

    /**
     * 获取表字段信息
     */
    @Test
    public void getColumnMetaData() {
        IClient client = ClientCache.getClient(DataSourceType.VERTICA.getVal());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("LOADER_TEST").build();
        List columnMetaData = client.getColumnMetaData(source, queryDTO);
        Assert.assertTrue(CollectionUtils.isNotEmpty(columnMetaData));
    }

    /**
     * 数据预览测试
     */
    @Test
    public void preview() {
        IClient client = ClientCache.getClient(DataSourceType.VERTICA.getVal());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("LOADER_TEST").build();
        List preview = client.getPreview(source, queryDTO);
        Assert.assertTrue(CollectionUtils.isNotEmpty(preview));
    }

    /**
     * 获取所有schema测试
     */
    @Test
    public void getAllDb() {
        IClient client = ClientCache.getClient(DataSourceType.VERTICA.getVal());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().build();
        List dbs = client.getAllDatabases(source, queryDTO);
        Assert.assertTrue(CollectionUtils.isNotEmpty(dbs));
    }

    /**
     * 获取指定schema下的table
     */
    @Test
    public void getTableBySchema(){
        IClient client = ClientCache.getClient(DataSourceType.VERTICA.getVal());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().schema("test").build();
        List tables = client.getTableListBySchema(source, queryDTO);
        Assert.assertTrue(CollectionUtils.isNotEmpty(tables));
    }
}
