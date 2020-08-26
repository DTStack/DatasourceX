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
 * @Date ：Created in 17:43 2020/7/9
 * @Description：Hbase 2 测试
 */
public class Hbase2Test {
    HbaseSourceDTO source = HbaseSourceDTO.builder()
            .url("172.16.10.104:2181,172.16.10.224:2181,172.16.10.252:2181")
            .path("/hbase")
            .build();

    @Test
    public void testCon() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.HBASE2.getPluginName());
        Boolean isConnected = client.testCon(source);
        if (Boolean.FALSE.equals(isConnected)) {
            throw new DtLoaderException("连接异常");
        }
    }

    @Test
    public void getTableList() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.HBASE2.getPluginName());
        List<String> tableList = client.getTableList(source, null);
        System.out.println(tableList.size());
    }
}
