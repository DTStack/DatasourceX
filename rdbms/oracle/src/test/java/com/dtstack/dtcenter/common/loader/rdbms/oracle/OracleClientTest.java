package com.dtstack.dtcenter.common.loader.rdbms.oracle;

import com.dtstack.dtcenter.common.exception.DtCenterDefException;
import com.dtstack.dtcenter.common.loader.rdbms.common.AbsRdbmsClient;
import com.dtstack.dtcenter.loader.dto.SourceDTO;
import org.junit.Test;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 13:54 2020/1/6
 * @Description：Oracle 客户端测试
 */
public class OracleClientTest {
    private static AbsRdbmsClient rdbsClient = new OracleClient();

    @Test
    public void testConnection() throws Exception {
        SourceDTO source = SourceDTO.builder()
                .url("jdbc:oracle:thin:@172.16.8.178:1521:xe")
                .username("dtstack")
                .password("abc123")
                .build();
        Boolean isConnected = rdbsClient.testCon(source);
        if (!isConnected) {
            throw new DtCenterDefException("数据源连接异常");
        }
    }
}
