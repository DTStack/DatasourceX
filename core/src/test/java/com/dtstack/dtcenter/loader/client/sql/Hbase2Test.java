package com.dtstack.dtcenter.loader.client.sql;

import com.dtstack.dtcenter.loader.cache.pool.config.PoolConfig;
import com.dtstack.dtcenter.loader.client.ClientCache;
import com.dtstack.dtcenter.loader.client.IClient;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.comparator.BinaryComparator;
import com.dtstack.dtcenter.loader.dto.comparator.RegexStringComparator;
import com.dtstack.dtcenter.loader.dto.filter.Filter;
import com.dtstack.dtcenter.loader.dto.filter.PageFilter;
import com.dtstack.dtcenter.loader.dto.filter.RowFilter;
import com.dtstack.dtcenter.loader.dto.filter.SingleColumnValueFilter;
import com.dtstack.dtcenter.loader.dto.source.HbaseSourceDTO;
import com.dtstack.dtcenter.loader.enums.CompareOp;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 17:43 2020/7/9
 * @Description：Hbase 2 测试
 */
public class Hbase2Test {
    HbaseSourceDTO source = HbaseSourceDTO.builder()
            .url("kudu1,kudu2,kudu3:2181")
            .path("/hbase")
            .poolConfig(new PoolConfig())
            .build();

    @Test
    public void testCon() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.HBASE2.getVal());
        Boolean isConnected = client.testCon(source);
        if (Boolean.FALSE.equals(isConnected)) {
            throw new DtLoaderException("连接异常");
        }
    }

    @Test
    public void getTableList() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.HBASE2.getVal());
        List<String> tableList = client.getTableList(source, null);
        System.out.println(tableList.size());
    }

    @Test
    public void getColumnMetaData() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.HBASE2.getVal());
        List dtstack = client.getColumnMetaData(source, SqlQueryDTO.builder().tableName("TEST").build());
        System.out.println(dtstack);
    }

    @Test
    public void executorQuery() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.HBASE2.getVal());
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
        IClient client = ClientCache.getClient(DataSourceType.HBASE2.getVal());
        List<List<Object>> result = client.getPreview(source, SqlQueryDTO.builder().tableName("TEST").previewNum(1).build());
        System.out.println(result);
    }
}
