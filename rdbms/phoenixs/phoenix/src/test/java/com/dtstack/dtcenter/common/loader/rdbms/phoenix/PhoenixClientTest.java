package com.dtstack.dtcenter.common.loader.rdbms.phoenix;

import com.dtstack.dtcenter.common.exception.DtCenterDefException;
import com.dtstack.dtcenter.common.loader.rdbms.common.AbsRdbmsClient;
import com.dtstack.dtcenter.loader.dto.SourceDTO;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import org.junit.Test;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 15:54 2020/2/27
 * @Description：Phoenix 测试类
 */
public class PhoenixClientTest {
    private static AbsRdbmsClient rdbsClient = new PhoenixClient();

    @Test
    public void testConnection() throws Exception {
        SourceDTO source = SourceDTO.builder()
                .url("jdbc:phoenix:node01,node02,node03:2181")
                .build();
        Boolean isConnected = rdbsClient.testCon(source);
        if (!isConnected) {
            throw new DtCenterDefException("数据源连接异常");
        }
    }
}
