package com.dtstack.dtcenter.common.loader.es;

import com.dtstack.dtcenter.common.exception.DtCenterDefException;
import com.dtstack.dtcenter.common.loader.common.AbsRdbmsClient;
import com.dtstack.dtcenter.loader.dto.source.ESSourceDTO;
import org.junit.Test;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 21:57 2020/2/27
 * @Description：ES 客户端测试
 */
public class EsClientTest {
    private static AbsRdbmsClient rdbsClient = new EsClient();
    ESSourceDTO source = ESSourceDTO.builder()
            .url("kudu5:9200")
            .username("elastic")
            .others("abc123")
            .build();

    @Test
    public void getConnFactory() throws Exception {

        Boolean isConnected = rdbsClient.testCon(source);
        if (!isConnected) {
            throw new DtCenterDefException("数据源连接异常");
        }
    }

    @Test
    public void getAllDatabase(){

    }
}
