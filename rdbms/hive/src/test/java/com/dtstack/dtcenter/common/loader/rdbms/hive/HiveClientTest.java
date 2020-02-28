package com.dtstack.dtcenter.common.loader.rdbms.hive;

import com.dtstack.dtcenter.common.loader.rdbms.common.AbsRdbmsClient;
import com.dtstack.dtcenter.loader.dto.ColumnMetaDTO;
import com.dtstack.dtcenter.loader.dto.SourceDTO;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import org.junit.Test;

import java.util.List;

public class HiveClientTest {
    private static AbsRdbmsClient rdbsClient = new HiveClient();

    @Test
    public void getConnFactory() throws Exception {
        SourceDTO source = SourceDTO.builder()
                .url("jdbc:hive2://cdh-impala2:10000/ceshis_pri")
                .username("root")
                .password("abc123")
                .build();
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("nanqi200228").filterPartitionColumns(false).build();
//        List<String> tableList = rdbsClient.getTableList(source, null);
        List<ColumnMetaDTO> columnMetaData = rdbsClient.getColumnMetaData(source, queryDTO);
        System.out.println(columnMetaData.size());
    }
}