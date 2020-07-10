package com.dtstack.dtcenter.common.loader.hbase;

import com.dtstack.dtcenter.common.exception.DtCenterDefException;
import com.dtstack.dtcenter.common.loader.common.AbsRdbmsClient;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.source.HbaseSourceDTO;
import org.junit.Test;

import java.util.List;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 17:07 2020/7/9
 * @Description：Hbase 客户端测试
 */
public class HbaseClientTest {
    private static AbsRdbmsClient rdbsClient = new HbaseClient();
    private String conf = "{\"hbase.zookeeper.quorum\":\"kudu1:2181,kudu2:2181,kudu3:2181\"," +
            "\"zookeeper.znode.parent\":\"/hbase\"}";
    private HbaseSourceDTO source = HbaseSourceDTO.builder().kerberosConfig(null)
            .config(conf).build();

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
