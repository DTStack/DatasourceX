package com.dtstack.dtcenter.loader.client.sql;

import com.dtstack.dtcenter.loader.IDownloader;
import com.dtstack.dtcenter.loader.client.AbsClientCache;
import com.dtstack.dtcenter.loader.client.IClient;
import com.dtstack.dtcenter.loader.dto.ColumnMetaDTO;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.source.Greenplum6SourceDTO;
import com.dtstack.dtcenter.loader.enums.ClientType;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import org.apache.commons.collections.CollectionUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.util.List;
import java.util.Map;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 03:41 2020/2/29
 * @Description：MySQL 5 测试
 */
public class Greenplum6Test {
    private static final AbsClientCache clientCache = ClientType.DATA_SOURCE_CLIENT.getClientCache();

    private static Greenplum6SourceDTO source = Greenplum6SourceDTO.builder()
            .url("jdbc:pivotal:greenplum://172.16.10.90:5432;DatabaseName=data")
            .username("gpadmin")
            .password("gpadmin")
            .schema("public")
            .build();

    @BeforeClass
    public static void beforeClass() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.GREENPLUM6.getPluginName());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("drop table if exists nanqi").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("create table nanqi (id integer, name text)").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("comment on table nanqi is 'table comment'").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("insert into nanqi values (1, 'nanqi')").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
    }

    @Test
    public void getCon() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.GREENPLUM6.getPluginName());
        Connection con = client.getCon(source);
        con.createStatement().close();
        con.close();
    }

    @Test
    public void testCon() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.GREENPLUM6.getPluginName());
        Boolean isConnected = client.testCon(source);
        if (Boolean.FALSE.equals(isConnected)) {
            throw new DtLoaderException("连接异常");
        }
    }

    @Test
    public void executeQuery() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.GREENPLUM6.getPluginName());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("select * from nanqi limit 2 ").build();
        List<Map<String, Object>> mapList = client.executeQuery(source, queryDTO);
        Assert.assertTrue(CollectionUtils.isNotEmpty(mapList));
    }

    @Test
    public void executeSqlWithoutResultSet() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.GREENPLUM6.getPluginName());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("select * from nanqi").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
    }

    @Test
    public void getTableList() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.GREENPLUM6.getPluginName());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().build();
        List<String> tableList = client.getTableList(source, queryDTO);
        Assert.assertTrue(CollectionUtils.isNotEmpty(tableList));
    }

    @Test
    public void getColumnClassInfo() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.GREENPLUM6.getPluginName());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("nanqi").build();
        List<String> columnClassInfo = client.getColumnClassInfo(source, queryDTO);
        Assert.assertTrue(CollectionUtils.isNotEmpty(columnClassInfo));
    }

    @Test
    public void getColumnMetaData() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.GREENPLUM6.getPluginName());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("nanqi").build();
        List<ColumnMetaDTO> columnMetaData = client.getColumnMetaData(source, queryDTO);
        Assert.assertTrue(CollectionUtils.isNotEmpty(columnMetaData));
    }

    @Test
    public void getTableMetaComment() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.GREENPLUM6.getPluginName());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("nanqi").build();
        String metaComment = client.getTableMetaComment(source, queryDTO);
        Assert.assertEquals("table comment", metaComment);
    }

    @Test
    public void getDownloader() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.GREENPLUM6.getPluginName());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("select * from nanqi").build();
        IDownloader downloader = client.getDownloader(source, queryDTO);
        Assert.assertTrue(CollectionUtils.isNotEmpty(downloader.getMetaInfo()));
    }

    @Test
    public void testGetPreview() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.GREENPLUM6.getPluginName());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("nanqi").previewNum(50).build();
        List preview = client.getPreview(source, queryDTO);
        Assert.assertTrue(CollectionUtils.isNotEmpty(preview));
    }

    @Test
    public void getAllDatabases() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.GREENPLUM6.getPluginName());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().build();
        List allDatabases = client.getAllDatabases(source, queryDTO);
        Assert.assertTrue(CollectionUtils.isNotEmpty(allDatabases));
    }

}
