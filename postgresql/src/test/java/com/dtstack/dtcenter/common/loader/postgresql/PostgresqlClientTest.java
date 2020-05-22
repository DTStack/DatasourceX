package com.dtstack.dtcenter.common.loader.postgresql;

import com.dtstack.dtcenter.common.exception.DtCenterDefException;
import com.dtstack.dtcenter.common.loader.common.AbsRdbmsClient;
import com.dtstack.dtcenter.loader.dto.source.PostgresqlSourceDTO;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import org.junit.Test;

import java.util.List;

public class PostgresqlClientTest {

    private static AbsRdbmsClient rdbsClient = new PostgresqlClient();
    private PostgresqlSourceDTO source = PostgresqlSourceDTO.builder()
            .url("jdbc:postgresql://172.16.8.193:5432/DTstack?currentSchema=xq_libra")
            .username("root")
            .password("123456")
            .schema("")
            .build();

    private SqlQueryDTO sqlQueryDTO = SqlQueryDTO.builder().view(true).build();


//    SourceDTO(url=jdbc:postgresql://172.16.8.193:5432/DTstack?currentSchema=xq_0918, master=null, brokerUrls=null, hostPort=null, schema=, username=root, password=123456, defaultFS=null, config=null, others=null, path=null, connectMode=null, protocol=null, auth=null, redisMode=null, connection=null, kerberosConfig={})
//SqlQueryDTO(sql=null, tableName=null, tableNamePattern=null, tableTypes=null, columns=null, view=true, filterPartitionColumns=false)
    @Test
    public void testConnection() throws Exception {
        Boolean isConnected = rdbsClient.testCon(source);
        if (!isConnected) {
            throw new DtCenterDefException("数据源连接异常");
        }
    }

    @Test
    public void getTableList() throws Exception {
        List tableList = rdbsClient.getTableList(source, sqlQueryDTO);
        System.out.println(tableList);
    }
}