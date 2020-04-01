package com.dtstack.dtcenter.common.loader.rdbms.hbase;

import com.dtstack.dtcenter.common.exception.DtCenterDefException;
import com.dtstack.dtcenter.common.loader.rdbms.common.AbsRdbmsClient;
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

    @Test
    public void getConnFactory() throws Exception {
        SourceDTO source = SourceDTO.builder()
                .url("172.16.100.105:2181")
                .build();
        Boolean isConnected = rdbsClient.testCon(source);
        if (!isConnected) {
            throw new DtCenterDefException("数据源连接异常");
        }
    }


    @Test
    public void getTableList() throws Exception {
        SourceDTO source = SourceDTO.builder()
                .url("172.16.100.105:2181")
                .build();
        List tableList = rdbsClient.getTableList(source, null);
        System.out.println(tableList);

    }

    @Test
    public void getColumnMetaData() throws Exception {
        SourceDTO source = SourceDTO.builder()
                .url("172.16.100.105:2181")
                .build();
        SqlQueryDTO sqlQueryDTO = SqlQueryDTO.builder().tableName("KYLIN_JDE128H9FD").build();
        List columnMetaData = rdbsClient.getColumnMetaData(source, sqlQueryDTO);
        System.out.println(columnMetaData);

    }
}
