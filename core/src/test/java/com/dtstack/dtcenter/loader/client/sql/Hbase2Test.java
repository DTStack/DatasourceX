package com.dtstack.dtcenter.loader.client.sql;

import com.dtstack.dtcenter.loader.client.ClientCache;
import com.dtstack.dtcenter.loader.client.IClient;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.comparator.RegexStringComparator;
import com.dtstack.dtcenter.loader.dto.filter.Filter;
import com.dtstack.dtcenter.loader.dto.filter.PageFilter;
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
            .url("172.16.8.107,172.16.8.108,172.16.8.109:2181")
            .path("/hbase")
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
        PageFilter pageFilter = new PageFilter(2);
        ArrayList<Filter> filters = new ArrayList<>();
        SingleColumnValueFilter filter = new SingleColumnValueFilter("baseInfo".getBytes(), "age".getBytes(), CompareOp.EQUAL, new RegexStringComparator("."));
        filter.setFilterIfMissing(true);
        filters.add(filter);
        filters.add(pageFilter);
        List list = client.executeQuery(source, SqlQueryDTO.builder().tableName("TEST").hbaseFilter(filters).build());
        System.out.println(list);
    }

    @Test
    public void preview() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.HBASE2.getVal());
        List<List<Object>> result = client.getPreview(source, SqlQueryDTO.builder().tableName("TEST").previewNum(1).build());
        System.out.println(result);
    }
}
