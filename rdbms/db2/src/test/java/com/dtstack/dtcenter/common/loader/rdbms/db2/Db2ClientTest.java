package com.dtstack.dtcenter.common.loader.rdbms.db2;

import com.dtstack.dtcenter.common.exception.DtCenterDefException;
import com.dtstack.dtcenter.common.loader.rdbms.common.AbsRdbmsClient;
import com.dtstack.dtcenter.loader.dto.SourceDTO;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import org.junit.Test;

import java.util.List;

public class Db2ClientTest {
    private static AbsRdbmsClient rdbsClient = new Db2Client();
    SourceDTO source = SourceDTO.builder()
            .url("jdbc:db2://kudu5:50000/xiaochen")
            .username("xiaochen")
            .password("Abc1234")
            .build();

        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("EMPLOYEE").build();
//    SqlQueryDTO queryDTO = SqlQueryDTO.builder().build();



    @Test
    public void getConnFactory() throws Exception {
        Boolean isConnected = rdbsClient.testCon(source);
        if (!isConnected) {
            throw new DtCenterDefException("数据源连接异常");
        }
    }

    @Test
    public void getTableList() throws Exception {
        List tableList = rdbsClient.getTableList(source, queryDTO);
        System.out.println(tableList);

    }

    @Test
    public void getTableMetaComment() throws Exception {

        String tableMetaComment = rdbsClient.getTableMetaComment(source, queryDTO);
        System.out.println(tableMetaComment);
    }

    @Test
    public void getColumnMetaData() throws Exception {
        List columnMetaData = rdbsClient.getColumnMetaData(source, queryDTO);
        System.out.println(columnMetaData);

    }
}