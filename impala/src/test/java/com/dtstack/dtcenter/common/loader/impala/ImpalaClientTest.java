package com.dtstack.dtcenter.common.loader.impala;

import com.dtstack.dtcenter.common.loader.common.AbsRdbmsClient;
import com.dtstack.dtcenter.loader.dto.ColumnMetaDTO;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.source.ImpalaSourceDTO;
import org.junit.Test;

import java.util.List;

public class ImpalaClientTest {
    private static AbsRdbmsClient rdbsClient = new ImpalaClient();

    @Test
    public void getConnFactory() throws Exception {
        ImpalaSourceDTO source = ImpalaSourceDTO.builder()
                .url("jdbc:impala://cdh-impala1:21050/ceshis_pri;AuthMech=3")
                .username("root")
                .password("abc123")
                .build();
        List<String> tableList = rdbsClient.getTableList(source, null);
        source.setConnection(null);
        List<ColumnMetaDTO> columnMetaData = rdbsClient.getColumnMetaData(source, SqlQueryDTO.builder().tableName(
                "nanqi200228").build());
        System.out.println(tableList.size());
    }
}