package com.dtstack.dtcenter.loader.client.sql;

import com.dtstack.dtcenter.loader.cache.pool.config.PoolConfig;
import com.dtstack.dtcenter.loader.client.BaseTest;
import com.dtstack.dtcenter.loader.client.ClientCache;
import com.dtstack.dtcenter.loader.client.IClient;
import com.dtstack.dtcenter.loader.client.IHbase;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.comparator.BinaryComparator;
import com.dtstack.dtcenter.loader.dto.filter.Filter;
import com.dtstack.dtcenter.loader.dto.filter.RowFilter;
import com.dtstack.dtcenter.loader.dto.source.HbaseSourceDTO;
import com.dtstack.dtcenter.loader.enums.CompareOp;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import org.apache.commons.collections.CollectionUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 17:43 2020/7/9
 * @Description：Hbase 2 测试
 */
public class HbaseTest extends BaseTest {

    // 构建client
    private static final IClient client = ClientCache.getClient(DataSourceType.HBASE2.getPluginName());

    // 构建hbase client
    private static final IHbase HBASE_CLIENT = ClientCache.getHbase(DataSourceType.HBASE2.getVal());

    // 连接池信息
    private static final PoolConfig poolConfig = PoolConfig.builder().maximumPoolSize(100).build();

    // 构建数据源信息
    private static final HbaseSourceDTO source = HbaseSourceDTO.builder()
            .url("172.16.100.175:2181,172.16.101.196:2181,172.16.101.227:2181")
            .path("/hbase2")
            .poolConfig(poolConfig)
            .build();

    /**
     * 数据准备
     */
    @BeforeClass
    public static void setUp () {
        try {
            HBASE_CLIENT.createHbaseTable(source, "loader_test", new String[]{"info"});
        } catch (Exception e) {
            // 目前没有支持没有删除表的方法
        }
        HBASE_CLIENT.putRow(source, "loader_test", UUID.randomUUID().toString(), "info", "uuid", UUID.randomUUID().toString());
        HBASE_CLIENT.putRow(source, "loader_test", UUID.randomUUID().toString(), "info", "uuid", UUID.randomUUID().toString());
        HBASE_CLIENT.putRow(source, "loader_test", UUID.randomUUID().toString(), "info", "uuid", UUID.randomUUID().toString());
    }

    @Test
    public void testCon() throws Exception {
        Boolean isConnected = client.testCon(source);
        if (Boolean.FALSE.equals(isConnected)) {
            throw new DtLoaderException("connection exception");
        }
    }

    @Test
    public void getTableList() {
        List<String> tableList = client.getTableList(source, null);
        Assert.assertTrue(CollectionUtils.isNotEmpty(tableList));
    }

    @Test
    public void getColumnMetaData() {
        List metaData = client.getColumnMetaData(source, SqlQueryDTO.builder().tableName("loader_test").build());
        Assert.assertTrue(CollectionUtils.isNotEmpty(metaData));
    }

    @Test
    public void executorQuery() {
        ArrayList<Filter> filters = new ArrayList<>();
        RowFilter hbaseRowFilter = new RowFilter(
                CompareOp.GREATER, new BinaryComparator("0".getBytes()));
        hbaseRowFilter.setReversed(true);
        filters.add(hbaseRowFilter);
        SqlQueryDTO sqlQueryDTO = SqlQueryDTO.builder().tableName("loader_test").hbaseFilter(filters).build();
        List result = client.executeQuery(source, sqlQueryDTO);
        Assert.assertTrue(CollectionUtils.isNotEmpty(result));
    }

    @Test
    public void preview() {
        List<List<Object>> result = client.getPreview(source, SqlQueryDTO.builder().tableName("loader_test").previewNum(2).build());
        Assert.assertTrue(CollectionUtils.isNotEmpty(result));
    }

    @Test
    public void getTableListBySchema() {
        List<String> result = client.getTableListBySchema(source, SqlQueryDTO.builder().schema("default").tableName("loader_test").build());
        Assert.assertTrue(CollectionUtils.isNotEmpty(result));
    }

    @Test
    public void getAllDatabases() {
        List<String> result = client.getAllDatabases(source, SqlQueryDTO.builder().build());
        Assert.assertTrue(CollectionUtils.isNotEmpty(result));
    }

    @Test(expected = DtLoaderException.class)
    public void getCon() {
        client.getCon(source);
    }

    @Test(expected = DtLoaderException.class)
    public void getColumnMetaDataWithSql() {
        client.getColumnMetaDataWithSql(source, SqlQueryDTO.builder().build());
    }

    @Test(expected = DtLoaderException.class)
    public void getFlinkColumnMetaData() {
        client.getFlinkColumnMetaData(source, SqlQueryDTO.builder().build());
    }

    @Test(expected = DtLoaderException.class)
    public void getTableMetaComment() {
        client.getTableMetaComment(source, SqlQueryDTO.builder().build());
    }

    @Test(expected = DtLoaderException.class)
    public void executeSqlWithoutResultSet() {
        client.executeSqlWithoutResultSet(source, SqlQueryDTO.builder().build());
    }

    @Test(expected = DtLoaderException.class)
    public void getColumnClassInfo() {
        client.getColumnClassInfo(source, SqlQueryDTO.builder().build());
    }

    @Test(expected = DtLoaderException.class)
    public void getDownloader() throws Exception {
        client.getDownloader(source, SqlQueryDTO.builder().build());
    }

    @Test(expected = DtLoaderException.class)
    public void getCreateTableSql() {
        client.getCreateTableSql(source, SqlQueryDTO.builder().build());
    }

    @Test(expected = DtLoaderException.class)
    public void getPartitionColumn() {
        client.getPartitionColumn(source, SqlQueryDTO.builder().build());
    }

    @Test(expected = DtLoaderException.class)
    public void getTable() {
        client.getTable(source, SqlQueryDTO.builder().build());
    }

    @Test(expected = DtLoaderException.class)
    public void getCurrentDatabase() {
        client.getCurrentDatabase(source);
    }

    @Test(expected = DtLoaderException.class)
    public void createDatabase() {
        client.createDatabase(source, "", "");
    }

    @Test(expected = DtLoaderException.class)
    public void isDatabaseExists() {
        client.isDatabaseExists(source, "");
    }

    @Test(expected = DtLoaderException.class)
    public void isTableExistsInDatabase() {
        client.isTableExistsInDatabase(source, "", "");
    }

}
