package com.dtstack.dtcenter.common.loader.clickhouse;

import com.dtstack.dtcenter.common.loader.common.AbsRdbmsClient;
import com.dtstack.dtcenter.loader.dto.ColumnMetaDTO;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.source.ClickHouseSourceDTO;
import org.junit.Test;

import java.util.List;

public class ClickhouseClientTest {
    private static AbsRdbmsClient rdbsClient = new ClickhouseClient();

    ClickHouseSourceDTO source = ClickHouseSourceDTO.builder()
            .url("jdbc:clickhouse://172.16.10.168:8123/mqTest")
            .username("dtstack")
            .password("abc123")
            .schema("mqTest")
            .build();
    SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("cust").build();

    @Test
    public void getConnFactory() throws Exception {
        List<ColumnMetaDTO> columnMetaData = rdbsClient.getColumnMetaData(source, queryDTO);
        System.out.println(columnMetaData.size());
    }

    @Test
    public void getAllDataBases() throws Exception{
        rdbsClient.getAllDatabases(source,queryDTO).forEach(s->{
            System.out.println(s);
        });
    }

    @Test
    public void getCreateSql() throws Exception{
        System.out.println(rdbsClient.getCreateTableSql(source,queryDTO));
    }

    @Test
    public void getPartitionColumn() throws Exception{
        System.out.println(rdbsClient.getPartitionColumn(source,queryDTO));
    }
}