package com.dtstack.dtcenter.loader.client.sql;

import com.dtstack.dtcenter.common.exception.DtCenterDefException;
import com.dtstack.dtcenter.loader.client.AbsClientCache;
import com.dtstack.dtcenter.loader.client.IClient;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.comparator.BinaryComparator;
import com.dtstack.dtcenter.loader.dto.filter.Filter;
import com.dtstack.dtcenter.loader.dto.filter.RowFilter;
import com.dtstack.dtcenter.loader.dto.source.HbaseSourceDTO;
import com.dtstack.dtcenter.loader.enums.ClientType;
import com.dtstack.dtcenter.loader.enums.CompareOp;
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
    private static final AbsClientCache clientCache = ClientType.DATA_SOURCE_CLIENT.getClientCache();

//    HbaseSourceDTO source = HbaseSourceDTO.builder()
//            .url("stream00,stream01,stream02:2181")
//            .path("/hbase")
//            .build();

    private String conf = "{\"hbase.zookeeper.quorum\":\"kudu1:2181,kudu2:2181,kudu3:2181\"," +
            "\"zookeeper.znode.parent\":\"/hbase\"}";
    private HbaseSourceDTO source = HbaseSourceDTO.builder().kerberosConfig(null)
            .config(conf).build();
    @Test
    public void testCon() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.HBASE2.getPluginName());
        Boolean isConnected = client.testCon(source);
        if (Boolean.FALSE.equals(isConnected)) {
            throw new DtCenterDefException("连接异常");
        }
    }

    @Test
    public void getTableList() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.HBASE2.getPluginName());
        List<String> tableList = client.getTableList(source, null);
        System.out.println(tableList);
    }

    @Test
    public void executorQuery() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.HBASE.getPluginName());
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
        System.out.println(list);
    }

    @Test
    public void preview() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.HBASE.getPluginName());
        List<List<Object>> result = client.getPreview(source, SqlQueryDTO.builder().tableName("dtstack").previewNum(1).build());
        System.out.println(result);
    }
}
