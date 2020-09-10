package com.dtstack.dtcenter.loader.client.sql;

import com.dtstack.dtcenter.loader.client.ClientCache;
import com.dtstack.dtcenter.loader.client.IClient;
import com.dtstack.dtcenter.loader.dto.source.HbaseSourceDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import org.junit.Test;

import java.util.List;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 23:36 2020/2/28
 * @Description：HBase 测试
 */
public class HbaseTest {
    HbaseSourceDTO source = HbaseSourceDTO.builder()
            .url("172.16.8.107,172.16.8.108,172.16.8.109:2181")
            .path("/hbase")
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
        System.out.println(tableList.size());
    }

    @Test
    public void getColumnMetaData() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.HBASE.getPluginName());
        List dtstack = client.getColumnMetaData(source, SqlQueryDTO.builder().tableName("dtstack").build());
        System.out.println(dtstack);
    }

    @Test
    public void executorQuery() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.HBASE.getPluginName());
        PageFilter pageFilter = new PageFilter(1);
        ArrayList<Filter> filters = new ArrayList<>();
        filters.add(pageFilter);
        String column = "info:sex";
        String column2 = "info:name";
        //String column2 = "liftInfo:girlFriend";
        ArrayList<String> columns = Lists.newArrayList(column,column2);
        SingleColumnValueFilter filter = new SingleColumnValueFilter("baseInfo".getBytes(), "age".getBytes(), CompareOp.EQUAL, new RegexStringComparator("."));
        //filter.setFilterIfMissing(true);
        //filters.add(filter);
        //filters.add(pageFilter);
        RowFilter rowFilter = new RowFilter(CompareOp.LESS_OR_EQUAL, new BinaryComparator("rowkey2".getBytes()));
        RowFilter rowFilter2 = new RowFilter(CompareOp.GREATER_OR_EQUAL, new BinaryComparator("rowkey1".getBytes()));
        //filters.add(rowFilter);
        //filters.add(rowFilter2);
        List list = client.executeQuery(source, SqlQueryDTO.builder().tableName("yy_test").columns(columns).build());
        System.out.println(list);
    }

    @Test
    public void preview() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.HBASE.getPluginName());
        List<List<Object>> result = client.getPreview(source, SqlQueryDTO.builder().tableName("yy_test").previewNum(10).build());
        System.out.println(result);
    }
}
