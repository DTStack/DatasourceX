package com.dtstack.dtcenter.loader.client.sql;

import com.dtstack.dtcenter.loader.cache.pool.config.PoolConfig;
import com.dtstack.dtcenter.loader.client.ClientCache;
import com.dtstack.dtcenter.loader.client.IClient;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.comparator.BinaryComparator;
import com.dtstack.dtcenter.loader.dto.filter.Filter;
import com.dtstack.dtcenter.loader.dto.filter.PageFilter;
import com.dtstack.dtcenter.loader.dto.filter.RowFilter;
import com.dtstack.dtcenter.loader.dto.filter.SingleColumnValueFilter;
import com.dtstack.dtcenter.loader.dto.filter.TimestampFilter;
import com.dtstack.dtcenter.loader.dto.source.HbaseSourceDTO;
import com.dtstack.dtcenter.loader.enums.CompareOp;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import org.apache.commons.collections.CollectionUtils;
import org.junit.Assert;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 23:36 2020/2/28
 * @Description：HBase 测试
 */
public class HbaseTest {
    HbaseSourceDTO source = HbaseSourceDTO.builder()
            .url("172.16.100.175:2181,172.16.101.196:2181,172.16.101.227:2181")
            .path("/hbase2")
            //.poolConfig(new PoolConfig())
            .build();

    @Test
    public void testCon() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.HBASE.getVal());
        Boolean isConnected = client.testCon(source);
        if (Boolean.FALSE.equals(isConnected)) {
            throw new DtLoaderException("连接异常");
        }
    }

    @Test
    public void getTableList() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.HBASE.getVal());
        List<String> tableList = client.getTableList(source, null);
        System.out.println(tableList);
    }

    @Test
    public void getColumnMetaData() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.HBASE.getVal());
        List dtstack = client.getColumnMetaData(source, SqlQueryDTO.builder().tableName("TEST").build());
        System.out.println(dtstack);
    }

    @Test
    public void executorQuery() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.HBASE.getVal());
//        PageFilter pageFilter = new PageFilter(2);
        ArrayList<Filter> filters = new ArrayList<>();
//        filters.add(pageFilter);
//        String column = "baseInfo:age";
//        String column2 = "liftInfo:girlFriend";
//        ArrayList<Object> columns = Lists.newArrayList(column, column2);
//        SingleColumnValueFilter filter = new SingleColumnValueFilter("baseInfo".getBytes(), "age".getBytes(), CompareOp.EQUAL, new RegexStringComparator("."));
//        filter.setFilterIfMissing(true);
//        filters.add(filter);
        RowFilter hbaseRowFilter = new RowFilter(
                CompareOp.EQUAL,new BinaryComparator("0_k".getBytes()));
        hbaseRowFilter.setReversed(true);
//        filters.add(pageFilter);
        filters.add(hbaseRowFilter);
        List list = client.executeQuery(source, SqlQueryDTO.builder().tableName("wuren_foo").hbaseFilter(filters).build());
        System.out.println(System.currentTimeMillis());
        List list1 = client.executeQuery(source, SqlQueryDTO.builder().tableName("wuren_foo").hbaseFilter(filters).build());
        System.out.println(System.currentTimeMillis());
        List list2 = client.executeQuery(source, SqlQueryDTO.builder().tableName("wuren_foo").hbaseFilter(filters).build());
        System.out.println(System.currentTimeMillis());
        List list3 = client.executeQuery(source, SqlQueryDTO.builder().tableName("wuren_foo").hbaseFilter(filters).build());
        System.out.println(System.currentTimeMillis());
        System.out.println(list);
    }

    @Test
    public void preview() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.HBASE.getVal());
        List<List<Object>> result = client.getPreview(source, SqlQueryDTO.builder().tableName("yl_assets_test:yl_create_table_test1").previewNum(100).build());
        System.out.println(result);
    }

    @Test
    public void timestampFilterTest_less_or_equal() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.HBASE.getVal());
        PageFilter pageFilter = new PageFilter(1);
        TimestampFilter timestampFilter = new TimestampFilter(CompareOp.LESS_OR_EQUAL, 1602506453868L);
        List<Filter> filters = Lists.newArrayList(timestampFilter, pageFilter);
        List<Map<String, Object>> result = client.executeQuery(source, SqlQueryDTO.builder().tableName("jmt_test_1602506450578").hbaseFilter(filters).build());
        Assert.assertTrue(((Long) result.get(0).get("timestamp")) <= 1602506453868L);
    }

    @Test
    public void timestampFilter_less() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.HBASE.getVal());
        PageFilter pageFilter = new PageFilter(1);
        TimestampFilter timestampFilter = new TimestampFilter(CompareOp.LESS, 1602506453868L);
        List<Filter> filters = Lists.newArrayList(timestampFilter, pageFilter);
        List<Map<String, Object>> result = client.executeQuery(source, SqlQueryDTO.builder().tableName("jmt_test_1602506450578").hbaseFilter(filters).build());
        Assert.assertTrue(((Long) result.get(0).get("timestamp")) < 1602506453868L);
    }

    @Test
    public void timestampFilter_equal() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.HBASE.getVal());
        PageFilter pageFilter = new PageFilter(1);
        TimestampFilter timestampFilter = new TimestampFilter(CompareOp.EQUAL, 1602506453868L);
        List<Filter> filters = Lists.newArrayList(timestampFilter, pageFilter);
        List<Map<String, Object>> result = client.executeQuery(source, SqlQueryDTO.builder().tableName("jmt_test_1602506450578").hbaseFilter(filters).build());
        Assert.assertEquals(1602506453868L, result.get(0).get("timestamp"));
    }

    @Test
    public void timestampFilterTest_granter() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.HBASE.getVal());
        PageFilter pageFilter = new PageFilter(100);
        TimestampFilter timestampFilter = new TimestampFilter(CompareOp.GREATER, 1602506453868L);
        List<Filter> filters = Lists.newArrayList(timestampFilter, pageFilter);
        List<Map<String, Object>> result = client.executeQuery(source, SqlQueryDTO.builder().tableName("jmt_test_1602506450578").hbaseFilter(filters).build());
        System.out.println(result);
        Assert.assertTrue(((Long) result.get(0).get("timestamp")) > 1602506453868L);
    }

    @Test
    public void timestampFilterTest_granter_or_equal() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.HBASE.getVal());
        PageFilter pageFilter = new PageFilter(100);
        TimestampFilter timestampFilter = new TimestampFilter(CompareOp.GREATER_OR_EQUAL, 1602506453868L);
        List<Filter> filters = Lists.newArrayList(timestampFilter, pageFilter);
        List<Map<String, Object>> result = client.executeQuery(source, SqlQueryDTO.builder().tableName("jmt_test_1602506450578").hbaseFilter(filters).build());
        System.out.println(result);
        Assert.assertTrue(((Long) result.get(0).get("timestamp")) >= 1602506453868L);
    }

    @Test
    public void timestampAndRowKeyFilterTest_granter_or_equal() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.HBASE.getVal());
        PageFilter pageFilter = new PageFilter(100);
        RowFilter rowFilter = new RowFilter(CompareOp.LESS, new BinaryComparator("auto9".getBytes()));
        TimestampFilter timestampFilter = new TimestampFilter(CompareOp.GREATER_OR_EQUAL, 1602506453868L);
        List<Filter> filters = Lists.newArrayList(timestampFilter, pageFilter, rowFilter);
        List<Map<String, Object>> result = client.executeQuery(source, SqlQueryDTO.builder().tableName("jmt_test_1602506450578").hbaseFilter(filters).build());
        Assert.assertTrue(((Long) result.get(0).get("timestamp")) >= 1602506453868L);
    }

    /**
     * 获取所有namespace
     */
    @Test
    public void getAllNamespace() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.HBASE.getVal());
        List<String> namespaces = client.getAllDatabases(source, SqlQueryDTO.builder().build());
        System.out.println(namespaces);
        Assert.assertTrue(CollectionUtils.isNotEmpty(namespaces));
    }

    /**
     * 获取指定namespace下的table
     */
    @Test
    public void getTableByNamespace() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.HBASE.getVal());
        List<String> tables = client.getTableListBySchema(source, SqlQueryDTO.builder().schema("default").build());
        System.out.println(tables);
        Assert.assertTrue(CollectionUtils.isNotEmpty(tables));
    }

    /**
     * 获取指定namespace下的table, namespace不存在测试
     */
    @Test(expected = DtLoaderException.class)
    public void getTableByNamespaceNotExists() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.HBASE.getVal());
        client.getTableListBySchema(source, SqlQueryDTO.builder().schema("xxx").build());
    }

}
