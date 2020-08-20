package com.dtstack.dtcenter.common.loader.mysql;

import com.dtstack.dtcenter.common.loader.common.AbsRdbmsClient;
import com.dtstack.dtcenter.loader.dto.source.Mysql8SourceDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import org.junit.Test;

public class MysqlClientTest {
    private static AbsRdbmsClient rdbsClient = new MysqlClient();

    @Test
    public void getConnFactory() throws Exception {
        Mysql8SourceDTO source = Mysql8SourceDTO.builder()
                .url("jdbc:mysql://localhost:3306?useSSL=false&serverTimezone=Asia/Shanghai")
                .username("root")
                .password("123456")
                .build();
        Boolean isConnected = rdbsClient.testCon(source);
        if (!isConnected) {
            throw new DtLoaderException("数据源连接异常");
        }
    }
}