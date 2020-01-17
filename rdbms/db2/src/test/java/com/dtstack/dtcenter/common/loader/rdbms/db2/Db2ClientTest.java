package com.dtstack.dtcenter.common.loader.rdbms.db2;

import com.dtstack.dtcenter.common.exception.DtCenterDefException;
import com.dtstack.dtcenter.common.loader.rdbms.common.AbsRdbmsClient;
import com.dtstack.dtcenter.loader.dto.SourceDTO;
import org.junit.Test;

public class Db2ClientTest {
    private static AbsRdbmsClient rdbsClient = new Db2Client();

    @Test
    public void getConnFactory() throws ClassNotFoundException {
        SourceDTO source = new SourceDTO.SourceDTOBuilder()
                .setUrl("jdbc:db2://kudu4:50000/dtstack")
                .setUsername("db2inst1")
                .setPassword("abc123")
                .builder();
        Boolean isConnected = rdbsClient.testCon(source);
        if (!isConnected) {
            throw new DtCenterDefException("数据源连接异常");
        }
    }
}