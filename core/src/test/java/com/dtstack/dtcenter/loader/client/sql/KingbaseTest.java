package com.dtstack.dtcenter.loader.client.sql;

import com.dtstack.dtcenter.loader.cache.pool.config.PoolConfig;
import com.dtstack.dtcenter.loader.client.ClientCache;
import com.dtstack.dtcenter.loader.client.IClient;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.source.KingbaseSourceDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import org.apache.commons.collections.CollectionUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.util.List;

/**
 * kingbase数据源测试类
 *
 * @author ：wangchuan
 * date：Created in 下午6:40 2020/9/2
 * company: www.dtstack.com
 */
public class KingbaseTest {

    // 构建client
    private static final IClient client = ClientCache.getClient(DataSourceType.KINGBASE8.getVal());

    // 构建数据源信息
    private static final KingbaseSourceDTO source = KingbaseSourceDTO.builder()
            .url("jdbc:kingbase8://172.16.100.186:54321/test_db")
            .username("test")
            .password("test123")
            .poolConfig(PoolConfig.builder().maximumPoolSize(2).build())
            .build();

    /**
     * 数据准备
     */
    @BeforeClass
    public static void beforeClass() {
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("drop table if exists LOADER_TEST").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("create table LOADER_TEST (id int, name varchar(50))").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("insert into LOADER_TEST values (1, 'LOADER_TEST')").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
    }

    /**
     * 获取连接 - 支持连接池测试
     * @throws Exception 异常
     */
    @Test
    public void getCon() throws Exception{
        Connection con1 = client.getCon(source);
        String con1JdbcConn = con1.toString().split("wrapping")[1];
        Connection con2 = client.getCon(source);
        con1.close();
        Connection con3 = client.getCon(source);
        String con3JdbcConn = con3.toString().split("wrapping")[1];
        assert con1JdbcConn.equals(con3JdbcConn);
        con2.close();
        con3.close();
    }

    /**
     * 测试连通性
     */
    @Test
    public void testCon() {
        client.testCon(source);
    }

    /**
     * 获取所有schema - 去除系统schema
     */
    @Test
    public void getAllDatabases() {
        List databases = client.getAllDatabases(source, SqlQueryDTO.builder().build());
        Assert.assertTrue(CollectionUtils.isNotEmpty(databases));
    }

    /**
     * 获取表字段信息 - 不指定schema
     * @throws Exception
     */
    @Test
    public void getColumnMetaData() {
        List metaData = client.getColumnMetaData(source, SqlQueryDTO.builder().tableName("LOADER_TEST").build());
        Assert.assertTrue(CollectionUtils.isNotEmpty(metaData));
    }

    /**
     * 获取表字段信息 - 不指定schema ：有注释的表
     */
    @Test
    public void getColumnMetaDataHiveComment() {
        List metaData = client.getColumnMetaData(source, SqlQueryDTO.builder().tableName("LOADER_TEST").build());
        Assert.assertTrue(CollectionUtils.isNotEmpty(metaData));
    }

    /**
     * 获取表字段信息 - 指定schema ，且表名中有
     */
    @Test
    public void getColumnMetaDataBySchema() {
        List metaData = client.getColumnMetaData(source, SqlQueryDTO.builder().tableName("LOADER_TEST").build());
        Assert.assertTrue(CollectionUtils.isNotEmpty(metaData));
    }

    /**
     * 获取字段 Java 类的标准名称 - 不指定schema
     */
    @Test
    public void getColumnClassInfo(){
        List result = client.getColumnClassInfo(source, SqlQueryDTO.builder().tableName("LOADER_TEST").build());
        Assert.assertTrue(CollectionUtils.isNotEmpty(result));
    }

    /**
     * 获取字段 Java 类的标准名称 - 指定schema
     */
    @Test
    public void getColumnClassInfoBySchema() {
        List result = client.getColumnClassInfo(source, SqlQueryDTO.builder().tableName("LOADER_TEST").build());
        Assert.assertTrue(CollectionUtils.isNotEmpty(result));
    }

    /**
     * 不指定schema获取表
     * @throws Exception
     */
    @Test
    public void getTableList() {
        List tableList = client.getTableList(source, SqlQueryDTO.builder().build());
        Assert.assertTrue(CollectionUtils.isNotEmpty(tableList));
    }

    /**
     * 获取指定schema下的表
     */
    @Test
    public void getTableListBySchema() {
        List tableList = client.getTableList(source, SqlQueryDTO.builder().build());
        Assert.assertTrue(CollectionUtils.isNotEmpty(tableList));
    }

    /**
     * 数据预览 - 不指定schema
     */
    @Test
    public void getPreview() {
        List preview = client.getPreview(source, SqlQueryDTO.builder().tableName("LOADER_TEST").previewNum(5).build());
        Assert.assertTrue(CollectionUtils.isNotEmpty(preview));
    }

    /**
     * 数据预览 - 指定schema， 且表名中有.存在
     */
    @Test
    public void getPreviewBySchema() {
        List dict = client.getPreview(source, SqlQueryDTO.builder().tableName("LOADER_TEST").previewNum(5).build());
        Assert.assertTrue(CollectionUtils.isNotEmpty(dict));
    }

    /**
     * 异常测试 - 数据预览 - 不指定schema， 且表名不再搜索范围内
     */
    @Test(expected = DtLoaderException.class)
    public void getPreviewException() {
        client.getPreview(source, SqlQueryDTO.builder().tableName("dev").previewNum(5).build());
    }

    /**
     * 自定义查询
     */
    @Test
    public void executeQuery() {
        client.executeQuery(source, SqlQueryDTO.builder().sql("select * from LOADER_TEST").build());
    }

    /**
     * 获取表注释
     */
    @Test
    public void getTableMetaComment() {
        String metaComment = client.getTableMetaComment(source, SqlQueryDTO.builder().tableName("LOADER_TEST").build());
        System.out.println(metaComment);
    }

    @Test
    public void getCurrentDatabase() {
        IClient client = ClientCache.getClient(DataSourceType.KINGBASE8.getVal());
        String currentDatabase = client.getCurrentDatabase(source);
        Assert.assertNotNull(currentDatabase);
    }
}
