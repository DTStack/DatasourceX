package com.dtstack.dtcenter.common.loader.oracle;

import com.dtstack.dtcenter.common.exception.DtCenterDefException;
import com.dtstack.dtcenter.common.loader.common.AbsRdbmsClient;
import com.dtstack.dtcenter.loader.dto.SourceDTO;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import org.junit.Test;

import java.util.List;

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

    @Test
    public void getColumnMetaDataTest() throws Exception {
        SourceDTO source = SourceDTO.builder()
                .url("jdbc:oracle:thin:@//118.31.39.174:1521/xe")
                .username("study")
                .password("study")
                .build();

        SqlQueryDTO sqlQueryDTO = SqlQueryDTO.builder().tableName("STUDY.AAAA").tableNamePattern("*").build();
        List columnMetaData = rdbsClient.getColumnMetaData(source, sqlQueryDTO);
        System.out.println(columnMetaData);

    }

}
