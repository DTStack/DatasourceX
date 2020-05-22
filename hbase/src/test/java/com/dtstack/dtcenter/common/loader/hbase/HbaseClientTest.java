package com.dtstack.dtcenter.common.loader.hbase;

import com.dtstack.dtcenter.common.exception.DtCenterDefException;
import com.dtstack.dtcenter.common.loader.common.AbsRdbmsClient;
import com.dtstack.dtcenter.loader.dto.SourceDTO;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import org.junit.Test;

import java.util.List;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 21:42 2020/2/27
 * @Description：Hbase 客户端测试
 */
public class HbaseClientTest {
    private static AbsRdbmsClient rdbsClient = new HbaseClient();
    private String conf = "{\"hbase.zookeeper.quorum\":\"172.16.10.104:2181,172.16.10.224:2181,172.16.10.252:2181\",\"zookeeper.znode.parent\":\"/hbase\"}";
    private SourceDTO source = SourceDTO.builder().kerberosConfig(null)
            .config(conf).build();

    private SourceDTO source2 = SourceDTO.builder().kerberosConfig(null).url("172.16.10.104:2181,172.16.10.224:2181,172.16.10.252:2181")
            .path("/hbase").build();


    @Test
    public void getConnFactory() throws Exception {
        Boolean isConnected = rdbsClient.testCon(source);
        if (!isConnected) {
            throw new DtCenterDefException("数据源连接异常");
        }
    }


    @Test
    public void getTableList() throws Exception {
        List tableList = rdbsClient.getTableList(source, null);
        System.out.println(tableList);
    }


    @Test
    public void getColumnMetaData() throws Exception {
        SqlQueryDTO sqlQueryDTO = SqlQueryDTO.builder().tableName("table2").build();
        List columnMetaData = rdbsClient.getColumnMetaData(source, sqlQueryDTO);
        System.out.println(columnMetaData);
    }
}
