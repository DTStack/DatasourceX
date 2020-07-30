package com.dtstack.dtcenter.common.loader.oracle;
import com.dtstack.dtcenter.common.exception.DtCenterDefException;
import com.dtstack.dtcenter.common.loader.common.AbsRdbmsClient;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.source.OracleSourceDTO;
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
        OracleSourceDTO source = OracleSourceDTO.builder()
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
        OracleSourceDTO source = OracleSourceDTO.builder()
                .url("jdbc:oracle:thin:@//118.31.39.174:1521/xe")
                .username("study")
                .password("study")
                .build();

        SqlQueryDTO sqlQueryDTO = SqlQueryDTO.builder().tableName("STUDY.AAAA").tableNamePattern("*").build();
        List columnMetaData = rdbsClient.getColumnMetaData(source, sqlQueryDTO);
        System.out.println(columnMetaData);

    }

    @Test
    public void getAllDatabases() throws Exception {
        OracleSourceDTO source = OracleSourceDTO.builder()
                .url("jdbc:oracle:thin:@172.16.8.193:1521:orcl")
                .username("system")
                .password("oracle")
                .build();
        List<String> databases = rdbsClient.getAllDatabases(source, SqlQueryDTO.builder().build());
        System.out.println(databases);
    }

    @Test
    public void getCreateTableSql() throws Exception {
        OracleSourceDTO source = OracleSourceDTO.builder()
                .url("jdbc:oracle:thin:@172.16.8.193:1521:orcl")
                .username("system")
                .password("oracle")
                .schema("MDSYS")
                .build();
        System.out.println(rdbsClient.getCreateTableSql(source, SqlQueryDTO.builder().tableName("SDO_CS_SRS").build()));
    }

}
