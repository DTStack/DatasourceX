package com.dtstack.dtcenter.common.loader.rdbms.oracle;

import com.dtstack.dtcenter.common.exception.DtCenterDefException;
import com.dtstack.dtcenter.common.loader.rdbms.common.AbsRdbmsClient;
import com.dtstack.dtcenter.loader.dto.SourceDTO;
import org.junit.Test;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 13:54 2020/1/6
 * @Description：TODO
 */
public class OracleClientTest {
    private static AbsRdbmsClient rdbsClient = new OracleClient();

    @Test
    public void testConnection() throws ClassNotFoundException {
        SourceDTO source = new SourceDTO.SourceDTOBuilder()
                .setUrl("jdbc:oracle:thin:@172.16.8.178:1521:xe")
                .setUsername("dtstack")
                .setPassword("abc123")
                .builder();
        Boolean isConnected = rdbsClient.testCon(source);
        if (!isConnected) {
            throw new DtCenterDefException("数据源连接异常");
        }
    }
}
