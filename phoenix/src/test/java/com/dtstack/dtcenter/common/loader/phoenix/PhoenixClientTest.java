package com.dtstack.dtcenter.common.loader.phoenix;

import com.dtstack.dtcenter.common.loader.common.AbsRdbmsClient;
import com.dtstack.dtcenter.loader.dto.source.PhoenixSourceDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
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
        PhoenixSourceDTO source = PhoenixSourceDTO.builder()
                .url("jdbc:phoenix:node01,node02,node03:2181")
                .build();
        Boolean isConnected = rdbsClient.testCon(source);
        if (!isConnected) {
            throw new DtLoaderException("数据源连接异常");
        }
    }
}
