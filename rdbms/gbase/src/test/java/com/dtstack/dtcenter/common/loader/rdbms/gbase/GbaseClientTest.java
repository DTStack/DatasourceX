package com.dtstack.dtcenter.common.loader.rdbms.gbase;

import com.dtstack.dtcenter.common.exception.DtCenterDefException;
import com.dtstack.dtcenter.common.loader.rdbms.common.AbsRdbmsClient;
import com.dtstack.dtcenter.loader.dto.SourceDTO;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import org.junit.Test;

import java.util.List;

public class GbaseClientTest {
    private static AbsRdbmsClient rdbsClient = new GbaseClient();

    private SourceDTO source = SourceDTO.builder()
            .url("jdbc:gbase://172.16.8.193:5258/dtstack")
            .username("root")
            .password("123456")
            .build();

    @Test
    public void getConnFactory() throws Exception {
        Boolean isConnected = rdbsClient.testCon(source);
        if (!isConnected) {
            throw new DtCenterDefException("数据源连接异常");
        }
    }

    @Test
    public void getTableList() throws Exception {
        List tableList = rdbsClient.getTableList(source, null);
        assert tableList != null;
    }

    @Test
    public void executeSqlWithoutResultSet() throws Exception {
        String createSQL = "CREATE TABLE nanqi (id int) comment 'table comment'";
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql(createSQL).build();
        rdbsClient.executeSqlWithoutResultSet(source, queryDTO);
    }

    @Test
    public void getTableMetaComment() throws Exception {
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("nanqi").build();
        String metaComment = rdbsClient.getTableMetaComment(source, queryDTO);
        assert metaComment != null;
    }
}