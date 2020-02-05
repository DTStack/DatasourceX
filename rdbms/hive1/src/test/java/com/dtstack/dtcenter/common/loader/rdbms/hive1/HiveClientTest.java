package com.dtstack.dtcenter.common.loader.rdbms.hive1;

import com.dtstack.dtcenter.common.exception.DtCenterDefException;
import com.dtstack.dtcenter.common.loader.rdbms.common.AbsRdbmsClient;
import com.dtstack.dtcenter.loader.dto.SourceDTO;
import org.junit.Test;

public class HiveClientTest {
    private static AbsRdbmsClient rdbsClient = new HiveClient();

    @Test
    public void getConnFactory() throws Exception {
        SourceDTO source = new SourceDTO.SourceDTOBuilder()
                .setUrl("jdbc:hive2://cdh-impala2:10000")
                .setUsername("root")
                .setPassword("abc123")
                .builder();
        Boolean isConnected = rdbsClient.testCon(source);
        if (!isConnected) {
            throw new DtCenterDefException("数据源连接异常");
        }
    }
}