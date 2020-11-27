package com.dtstack.dtcenter.loader.client.sql;

import com.dtstack.dtcenter.loader.client.ClientCache;
import com.dtstack.dtcenter.loader.client.IClient;
import com.dtstack.dtcenter.loader.dto.ColumnMetaDTO;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.source.PhoenixSourceDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.sql.Connection;
import java.util.List;
import java.util.Map;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 03:50 2020/2/29
 * @Description：Phoenix 测试
 */
@Ignore
public class PhoenixTest {
    private static PhoenixSourceDTO source = PhoenixSourceDTO.builder()
            .url("jdbc:phoenix:kudu1,kudu2,kudu3:2181")
            .username("root")
            .password("flink123")
            .build();

    @BeforeClass
    public static void beforeClass() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.Phoenix.getVal());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("drop table if exists NANQI").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("create table NANQI (id INTEGER, name VARCHAR CONSTRAINT my_pk PRIMARY KEY (id))").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("UPSERT into NANQI(id,name) values (1,'nanqi')").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
    }

    @Test
    public void getCon() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.Phoenix.getVal());
        Connection con = client.getCon(source);
        con.createStatement().close();
        con.close();
    }

    @Test
    public void testCon() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.Phoenix.getVal());
        Boolean isConnected = client.testCon(source);
        if (Boolean.FALSE.equals(isConnected)) {
            throw new DtLoaderException("连接异常");
        }
    }

    @Test
    public void executeQuery() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.Phoenix.getVal());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("select * from NANQI").build();
        List<Map<String, Object>> mapList = client.executeQuery(source, queryDTO);
        System.out.println(mapList.size());
    }

    @Test
    public void executeSqlWithoutResultSet() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.Phoenix.getVal());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("select * from NANQI").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
    }

    @Test
    public void getTableList() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.Phoenix.getVal());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().build();
        List<String> tableList = client.getTableList(source, queryDTO);
        System.out.println(tableList);
    }

    @Test
    public void getColumnClassInfo() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.Phoenix.getVal());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("NANQI").build();
        List<String> columnClassInfo = client.getColumnClassInfo(source, queryDTO);
        System.out.println(columnClassInfo.size());
    }

    @Test
    public void getColumnMetaData() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.Phoenix.getVal());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("NANQI").build();
        List<ColumnMetaDTO> columnMetaData = client.getColumnMetaData(source, queryDTO);
        System.out.println(columnMetaData.size());
    }

    @Test
    public void getTableMetaComment() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.Phoenix.getVal());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("NANQI").build();
        String metaComment = client.getTableMetaComment(source, queryDTO);
        System.out.println(metaComment);
    }
}
