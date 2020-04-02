package com.dtstack.dtcenter.common.loader.rdbms.kudu;

import com.dtstack.dtcenter.common.exception.DtCenterDefException;
import com.dtstack.dtcenter.common.loader.rdbms.common.AbsRdbmsClient;
import com.dtstack.dtcenter.loader.dto.SourceDTO;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import org.junit.Test;

import java.util.List;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 22:16 2020/2/27
 * @Description：Kudu 客户端测试
 */
public class KuduClientTest {
    private static AbsRdbmsClient rdbsClient = new KuduClient();

    @Test
    public void getConnFactory() throws Exception {
        SourceDTO source = SourceDTO.builder()
                .url("172.16.100.213:7051,172.16.101.252:7051")
                .build();
        Boolean isConnected = rdbsClient.testCon(source);
        if (!isConnected) {
            throw new DtCenterDefException("数据源连接异常");
        }
    }

    @Test
    public void getTableList() throws Exception {
        SourceDTO source = SourceDTO.builder()
                .url("172.16.100.213:7051,172.16.101.252:7051")
                .build();
        List tableList = rdbsClient.getTableList(source, null);
        System.out.println(tableList);

    }

    @Test
    public void getColumnMetaData() throws Exception {
        SourceDTO source = SourceDTO.builder()
                .url("172.16.100.213:7051,172.16.101.252:7051")
                .build();
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("impala::hxbho_pri.kudu100").build();
        List columnMetaData = rdbsClient.getColumnMetaData(source, queryDTO);
        System.out.println(columnMetaData);

    }



}
