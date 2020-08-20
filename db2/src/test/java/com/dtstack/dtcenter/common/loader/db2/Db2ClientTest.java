package com.dtstack.dtcenter.common.loader.db2;

import com.dtstack.dtcenter.common.loader.common.AbsRdbmsClient;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.source.Db2SourceDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;

public class Db2ClientTest {
    private static AbsRdbmsClient rdbsClient = new Db2Client();
    Db2SourceDTO source = Db2SourceDTO.builder()
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
            throw new DtLoaderException("数据源连接异常");
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

    @Test
    public void insert() throws Exception {
        Connection connection = DriverManager.getConnection("jdbc:db2://172.16.10.251:50000/mqTest", "DB2INST1", "abc123");
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("show tables");
        while (resultSet.next()){
            String string = resultSet.getString(1);
            System.out.println(string);
        }
    }

    @Test
    public void getDatabases() throws Exception {
        List<String> dataBases = rdbsClient.getAllDatabases(source,queryDTO);
        dataBases.forEach(s->{
            System.out.println(s);
        });
    }

    @Test
    public void getCreateSql() throws Exception {
        System.out.println(rdbsClient.getCreateTableSql(source,queryDTO));
    }
}