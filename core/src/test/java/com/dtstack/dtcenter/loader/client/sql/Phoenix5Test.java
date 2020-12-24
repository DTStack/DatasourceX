package com.dtstack.dtcenter.loader.client.sql;

import com.dtstack.dtcenter.loader.client.ClientCache;
import com.dtstack.dtcenter.loader.client.IClient;
import com.dtstack.dtcenter.loader.dto.ColumnMetaDTO;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.source.Phoenix5SourceDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import org.junit.Ignore;
import org.junit.Test;

import java.sql.Connection;
import java.util.List;
import java.util.Map;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 17:13 2020/7/9
 * @Description：Phoenix 测试
 */
@Ignore
public class Phoenix5Test {
    Phoenix5SourceDTO source = Phoenix5SourceDTO.builder()
            .url("jdbc:phoenix:172.16.100.174,172.16.100.220,172.16.100.181:2181")
            //.schema("default")
            .username("root")
            .password("flink123")
            .build();



    @Test
    public void getCon() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.PHOENIX5.getVal());
        Connection con = client.getCon(source);
        con.createStatement().close();
        con.close();
    }

    @Test
    public void testCon() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.PHOENIX5.getVal());
        Boolean isConnected = client.testCon(source);
        if (Boolean.FALSE.equals(isConnected)) {
            throw new DtLoaderException("连接异常");
        }
    }

    @Test
    public void executeQuery() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.PHOENIX5.getVal());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("SELECT * FROM TEST.TEST").build();
        List<Map<String, Object>> mapList = client.executeQuery(source, queryDTO);
        System.out.println(mapList);
    }

    @Test
    public void executeSqlWithoutResultSet() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.PHOENIX5.getVal());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("select * from \"DBAML\".\"ODL_TRADER_PF\" limit 2000").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
    }

    @Test
    public void getTableList() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.PHOENIX5.getVal());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().view(true).build();
        List<String> tableList = client.getTableList(source, queryDTO);
        System.out.println(tableList);
    }

    @Test
    public void getColumnClassInfo() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.PHOENIX5.getVal());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("\"DBAML\".\"ODL_TRADER_PF\"").build();
        List<String> columnClassInfo = client.getColumnClassInfo(source, queryDTO);
        System.out.println(columnClassInfo.size());
    }

    @Test
    public void getColumnMetaData() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.PHOENIX5.getVal());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("\"DBAML\".\"ODL_TRADER_PF\"").build();
        List<ColumnMetaDTO> columnMetaData = client.getColumnMetaData(source, queryDTO);
        System.out.println(columnMetaData.size());
    }

    @Test
    public void getTableMetaComment() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.PHOENIX5.getVal());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("\"DBAML\".\"ODL_TRADER_PF\"").build();
        String metaComment = client.getTableMetaComment(source, queryDTO);
        System.out.println(metaComment);
    }

    @Test
    public void getAllSchema() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.PHOENIX5.getVal());
        List allSchema = client.getAllDatabases(source, SqlQueryDTO.builder().build());
        System.out.println(allSchema);
    }

    @Test
    public void getTablesBySchema() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.PHOENIX5.getVal());
        List tables = client.getTableListBySchema(source, SqlQueryDTO.builder().schema("TEST").build());
        System.out.println(tables);
    }
}
