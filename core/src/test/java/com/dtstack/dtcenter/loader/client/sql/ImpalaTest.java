package com.dtstack.dtcenter.loader.client.sql;

import com.dtstack.dtcenter.loader.IDownloader;
import com.dtstack.dtcenter.loader.cache.pool.config.PoolConfig;
import com.dtstack.dtcenter.loader.client.ClientCache;
import com.dtstack.dtcenter.loader.client.IClient;
import com.dtstack.dtcenter.loader.dto.ColumnMetaDTO;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.Table;
import com.dtstack.dtcenter.loader.dto.source.ImpalaSourceDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

/**
 * @company: www.dtstack.com
 * @Author ：loader_test
 * @Date ：Created in 01:23 2020/2/29
 * @Description：Impala 测试
 */
public class ImpalaTest {
    private static ImpalaSourceDTO source = ImpalaSourceDTO.builder()
            .url("jdbc:impala://172.16.101.17:21050/default")
            .defaultFS("hdfs://ns1")
            .poolConfig(new PoolConfig())
            .build();

    @BeforeClass
    public static void beforeClass() {
        IClient client = ClientCache.getClient(DataSourceType.IMPALA.getVal());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("drop table if exists loader_test2").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("create table loader_test2(id int, name string)").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("drop table if exists loader_test3").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("create table loader_test3(id int, name string) COMMENT 'table comment' row format delimited fields terminated by ','").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("insert into loader_test2 values(1, 'loader_test')").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("drop table if exists aa_aa.shop_info").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("create table aa_aa.shop_info(id int, name string)").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("drop table if exists loader_download").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("create table loader_download(id int, name string) COMMENT 'table comment' row format delimited fields terminated by ','").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
        List<String> data = Lists.newArrayList();
        for (int i = 0; i < 501; i++) {
            data.add("(" + i + ",'loader_test')");
        }
        String join = String.join(",", data);
        queryDTO = SqlQueryDTO.builder().sql("insert into loader_download values " + join).build();
        client.executeSqlWithoutResultSet(source, queryDTO);
    }

    @Test
    public void getTableList_0001() throws SQLException {
        ImpalaSourceDTO source = ImpalaSourceDTO.builder()
                .url("jdbc:impala://172.16.101.17:21050/default")
                .defaultFS("hdfs://ns1")
                .schema("aa_aa")
                .build();
        IClient client = ClientCache.getClient(DataSourceType.IMPALA.getVal());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().build();
        List<String> tableList = client.getTableList(source, queryDTO);
        Assert.assertTrue(CollectionUtils.isNotEmpty(tableList));
    }
    @Test
    public void getColumnMetaData_0002() {
        ImpalaSourceDTO source = ImpalaSourceDTO.builder()
                .url("jdbc:impala://172.16.101.17:21050/default")
                .defaultFS("hdfs://ns1")
                .schema("aa_aa")
                .build();
        IClient client = ClientCache.getClient(DataSourceType.IMPALA.getVal());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("aa_aa.shop_info").build();
        List<ColumnMetaDTO> tableList = client.getColumnMetaData(source, queryDTO);
        Assert.assertTrue(CollectionUtils.isNotEmpty(tableList));
    }

    @Test
    public void getColumnMetaData_0003() {
        ImpalaSourceDTO source = ImpalaSourceDTO.builder()
                .url("jdbc:impala://172.16.101.17:21050/default")
                .defaultFS("hdfs://ns1")
                .schema("aa_aa")
                .build();
        IClient client = ClientCache.getClient(DataSourceType.IMPALA.getVal());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().schema("aa_aa").tableName("aa_aa.shop_info").build();
        List<ColumnMetaDTO> tableList = client.getColumnMetaData(source, queryDTO);
        Assert.assertTrue(CollectionUtils.isNotEmpty(tableList));
    }


    @Test
    public void getTableList() {
        IClient client = ClientCache.getClient(DataSourceType.IMPALA.getVal());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().build();
        List<String> tableList = client.getTableList(source, queryDTO);
        Assert.assertTrue(CollectionUtils.isNotEmpty(tableList));
    }


    @Test
    public void getCon() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.IMPALA.getVal());
        Connection con1 = client.getCon(source);
        con1.close();
    }

    @Test
    public void testCon() {
        IClient client = ClientCache.getClient(DataSourceType.IMPALA.getVal());
        Boolean isConnected = client.testCon(source);
        if (Boolean.FALSE.equals(isConnected)) {
            throw new DtLoaderException("连接异常");
        }
    }

    @Test
    public void executeQuery() {
        IClient client = ClientCache.getClient(DataSourceType.IMPALA.getVal());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("show databases").build();
        List<Map<String, Object>> mapList = client.executeQuery(source, queryDTO);
        Assert.assertTrue(CollectionUtils.isNotEmpty(mapList));
    }

    @Test
    public void execute() {
        IClient client = ClientCache.getClient(DataSourceType.IMPALA.getVal());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().schema("aa_aa").tableName("shop_info").build();
        List<Map<String, Object>> mapList = client.getColumnMetaData(source, queryDTO);
        Assert.assertTrue(CollectionUtils.isNotEmpty(mapList));
    }

    @Test
    public void execute1() {
        ImpalaSourceDTO source = ImpalaSourceDTO.builder()
                .url("jdbc:impala://172.16.101.17:21050/aa_aa")
                .defaultFS("hdfs://ns1")
                .poolConfig(new PoolConfig())
                .build();
        IClient client = ClientCache.getClient(DataSourceType.IMPALA.getVal());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().build();
        List<Map<String, Object>> mapList = client.getTableList(source, queryDTO);
        Assert.assertTrue(CollectionUtils.isNotEmpty(mapList));
    }

    @Test
    public void getTable_0001() {
        IClient client = ClientCache.getClient(DataSourceType.IMPALA.getVal());
        Table table = client.getTable(source, SqlQueryDTO.builder().schema("aa_aa").tableName("shop_info").build());
        Assert.assertNotNull(table);
    }

    @Test
    public void executeSqlWithoutResultSet() {
        IClient client = ClientCache.getClient(DataSourceType.IMPALA.getVal());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("show tables").build();
        Assert.assertTrue(client.executeSqlWithoutResultSet(source, queryDTO));
    }

    @Test
    public void getColumnClassInfo() {
        IClient client = ClientCache.getClient(DataSourceType.IMPALA.getVal());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("loader_test2").build();
        List<String> columnClassInfo = client.getColumnClassInfo(source, queryDTO);
        Assert.assertTrue(CollectionUtils.isNotEmpty(columnClassInfo));
    }

    @Test
    public void getColumnMetaData() {
        IClient client = ClientCache.getClient(DataSourceType.IMPALA.getVal());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("loader_test2").build();
        List<ColumnMetaDTO> columnMetaData = client.getColumnMetaData(source, queryDTO);
        Assert.assertTrue(CollectionUtils.isNotEmpty(columnMetaData));
    }

    @Test
    public void getTableMetaComment() {
        IClient client = ClientCache.getClient(DataSourceType.IMPALA.getVal());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("loader_test2").build();
        String tableMetaComment = client.getTableMetaComment(source, queryDTO);
        Assert.assertTrue(StringUtils.isEmpty(tableMetaComment));
    }

    @Test
    public void getPreview() {
        IClient client = ClientCache.getClient(DataSourceType.IMPALA.getVal());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().previewNum(2).tableName("loader_test2").build();
        List preview = client.getPreview(source, queryDTO);
        Assert.assertTrue(CollectionUtils.isNotEmpty(preview));
    }

    @Test
    public void getAllDatabases() {
        IClient client = ClientCache.getClient(DataSourceType.IMPALA.getVal());
        Assert.assertTrue(CollectionUtils.isNotEmpty(client.getAllDatabases(source, SqlQueryDTO.builder().build())));
    }

    @Test
    public void getPartitionColumn() {
        IClient client = ClientCache.getClient(DataSourceType.IMPALA.getVal());
        List<ColumnMetaDTO>  list = client.getPartitionColumn(source, SqlQueryDTO.builder().tableName("loader_test2").build());
        Assert.assertTrue(CollectionUtils.isEmpty(list));
    }

    @Test
    public void getCreateSql() {
        IClient client = ClientCache.getClient(DataSourceType.IMPALA.getVal());
        Assert.assertNotNull(client.getCreateTableSql(source, SqlQueryDTO.builder().tableName("loader_test2").build()));
    }

    @Test
    public void getTable() {
        IClient client = ClientCache.getClient(DataSourceType.IMPALA.getVal());
        Table table = client.getTable(source, SqlQueryDTO.builder().tableName("loader_test2").build());
        Assert.assertNotNull(table);
    }

    @Test
    public void getCurrentDatabase() {
        IClient client = ClientCache.getClient(DataSourceType.IMPALA.getVal());
        String currentDatabase = client.getCurrentDatabase(source);
        Assert.assertNotNull(currentDatabase);
    }

    @Test
    public void downloadBySql () throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.IMPALA.getVal());
        IDownloader downloader = client.getDownloader(source, SqlQueryDTO.builder().sql("select * from loader_download").build());
        Assert.assertEquals(2, downloader.getMetaInfo().size());
        while (!downloader.reachedEnd()) {
            List<List> result = (List<List>) downloader.readNext();
            Assert.assertTrue(CollectionUtils.isNotEmpty(result));
        }
        downloader.close();
    }
}
