package com.dtstack.dtcenter.common.loader.clickhouse;

import com.dtstack.dtcenter.common.loader.common.AbsRdbmsClient;
import com.dtstack.dtcenter.loader.dto.ColumnMetaDTO;
import com.dtstack.dtcenter.loader.dto.SourceDTO;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import org.junit.Test;

import java.util.List;

public class ClickhouseClientTest {
    private static AbsRdbmsClient rdbsClient = new ClickhouseClient();

    @Test
    public void getConnFactory() throws Exception {
        SourceDTO source = SourceDTO.builder()
                .url("jdbc:clickhouse://172.16.10.168:8123/mqTest")
                .username("dtstack")
                .password("abc123")
                .build();
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("mqresult2").build();
        List<ColumnMetaDTO> columnMetaData = rdbsClient.getColumnMetaData(source, queryDTO);
        System.out.println(columnMetaData.size());
    }
}