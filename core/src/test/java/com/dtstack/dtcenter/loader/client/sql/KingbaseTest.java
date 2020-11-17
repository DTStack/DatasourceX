package com.dtstack.dtcenter.loader.client.sql;

import com.dtstack.dtcenter.loader.cache.pool.config.PoolConfig;
import com.dtstack.dtcenter.loader.client.ClientCache;
import com.dtstack.dtcenter.loader.client.IClient;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.source.KingbaseSourceDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.dtstack.dtcenter.loader.source.DataSourceType;
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

    IClient client = ClientCache.getClient(DataSourceType.KINGBASE8.getVal());

    private static KingbaseSourceDTO source = KingbaseSourceDTO.builder()
            .url("jdbc:kingbase8://172.16.8.182:54321/dev")
            .username("admin")
            .password("Abc123")
            .poolConfig(PoolConfig.builder().maximumPoolSize(2).build())
            .build();

    @BeforeClass
    public static void beforeClass() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.KINGBASE8.getVal());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("drop table if exists nanqi").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("create table nanqi (id int, name varchar(50))").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("insert into nanqi values (1, 'nanqi')").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
    }

    /**
     * 获取连接 - 支持连接池测试
     * @throws Exception
     */
    @Test
    public void getCon() throws Exception{
        KingbaseSourceDTO source = KingbaseSourceDTO.builder()
                .url("jdbc:kingbase8://172.16.8.182:54321/ide")
                .username("admin")
                .password("Abc123")
                .poolConfig(PoolConfig.builder().maximumPoolSize(2).build())
                .build();
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
    public void testCon() throws Exception {
        client.testCon(source);
    }

    /**
     * 获取所有schema - 去除系统schema
     */
    @Test
    public void getAllDatabases() throws Exception {
        List databases = client.getAllDatabases(source, SqlQueryDTO.builder().build());
        System.out.println(databases);
    }

    /**
     * 获取表字段信息 - 不指定schema
     * @throws Exception
     */
    @Test
    public void getColumnMetaData() throws Exception {
        List metaData = client.getColumnMetaData(source, SqlQueryDTO.builder().tableName("nanqi").build());
        System.out.println(metaData);
    }

    /**
     * 获取表字段信息 - 不指定schema ：有注释的表
     * @throws Exception
     */
    @Test
    public void getColumnMetaDataHiveComment() throws Exception {
        List metaData = client.getColumnMetaData(source, SqlQueryDTO.builder().tableName("nanqi").build());
        System.out.println(metaData);
    }

    /**
     * 获取表字段信息 - 指定schema ，且表名中有.
     * @throws Exception
     */
    @Test
    public void getColumnMetaDataBySchema() throws Exception {
        List metaData = client.getColumnMetaData(source, SqlQueryDTO.builder().tableName("nanqi").build());
        System.out.println(metaData);
    }

    /**
     * 获取字段 Java 类的标准名称 - 不指定schema
     */
    @Test
    public void getColumnClassInfo() throws Exception {
        List rdos_dict = client.getColumnClassInfo(source, SqlQueryDTO.builder().tableName("nanqi").build());
        System.out.println(rdos_dict);
    }

    /**
     * 获取字段 Java 类的标准名称 - 指定schema
     */
    @Test
    public void getColumnClassInfoBySchema() throws Exception {
        List rdos_dict = client.getColumnClassInfo(source, SqlQueryDTO.builder().tableName("nanqi").build());
        System.out.println(rdos_dict);
    }

    /**
     * 不指定schema获取表
     * @throws Exception
     */
    @Test
    public void getTableList() throws Exception {
        List tableList = client.getTableList(source, SqlQueryDTO.builder().build());
        System.out.println(tableList);
    }

    /**
     * 获取指定schema下的表
     * @throws Exception
     */
    @Test
    public void getTableListBySchema() throws Exception {
        List tableList = client.getTableList(source, SqlQueryDTO.builder().build());
        System.out.println(tableList);
    }

    /**
     * 数据预览 - 不指定schema
     */
    @Test
    public void getPreview() throws Exception {
        List dict = client.getPreview(source, SqlQueryDTO.builder().tableName("nanqi").previewNum(5).build());
        System.out.println(dict);
    }

    /**
     * 数据预览 - 指定schema， 且表名中有.存在
     */
    @Test
    public void getPreviewBySchema() throws Exception {
        List dict = client.getPreview(source, SqlQueryDTO.builder().tableName("nanqi").previewNum(5).build());
        System.out.println(dict);
    }

    /**
     * 异常测试 - 数据预览 - 不指定schema， 且表名不再搜索范围内
     */
    @Test(expected = DtLoaderException.class)
    public void getPreviewException() throws Exception {
        client.getPreview(source, SqlQueryDTO.builder().tableName("dev").previewNum(5).build());
    }

    /**
     * 自定义查询
     * @throws Exception
     */
    @Test
    public void executeQuery() throws Exception {
        client.executeQuery(source, SqlQueryDTO.builder().sql("select * from nanqi").build());
    }

    /**
     * 获取表注释
     */
    @Test
    public void getTableMetaComment() throws Exception {
        String metaComment = client.getTableMetaComment(source, SqlQueryDTO.builder().tableName("nanqi").build());
        System.out.println(metaComment);
    }

    @Test
    public void getCurrentDatabase() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.KINGBASE8.getPluginName());
        String currentDatabase = client.getCurrentDatabase(source);
        Assert.assertNotNull(currentDatabase);
    }
}
