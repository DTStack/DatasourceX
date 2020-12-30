package com.dtstack.dtcenter.loader.client.sql;

import com.dtstack.dtcenter.loader.IDownloader;
import com.dtstack.dtcenter.loader.client.ClientCache;
import com.dtstack.dtcenter.loader.client.IClient;
import com.dtstack.dtcenter.loader.dto.ColumnMetaDTO;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.source.Db2SourceDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Connection;
import java.util.List;
import java.util.Map;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 22:30 2020/2/28
 * @Description：DB2 测试
 */
@Slf4j
public class Db2Test {
    private static Db2SourceDTO source = Db2SourceDTO.builder()
            .url("jdbc:db2://172.16.101.246:50002/DT_TEST")
            .username("db2inst1")
            .password("dtstack1")
            //.schema("SAMPLE")
            //.poolConfig(new PoolConfig())
            .build();

    //@BeforeClass
    public static void beforeClass() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.DB2.getVal());
        SqlQueryDTO queryDTO = null;
        try {
            // DB2 没找到 if exists 语法
            queryDTO = SqlQueryDTO.builder().sql("drop table nanqi").build();
            client.executeSqlWithoutResultSet(source, queryDTO);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }

        queryDTO = SqlQueryDTO.builder().sql("create table nanqi (id int, name varchar(50))").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("comment on table nanqi is 'table comment'").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("insert into nanqi values (1, 'nanqi')").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
    }

    @Test
    public void getCon() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.DB2.getVal());
        Connection con1 = client.getCon(source);
        con1.close();
    }

    @Test
    public void testCon() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.DB2.getVal());
        Boolean isConnected = client.testCon(source);
        if (Boolean.FALSE.equals(isConnected)) {
            throw new DtLoaderException("连接异常");
        }
    }

    @Test
    public void executeQuery() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.DB2.getVal());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("select * from nanqi limit 1,1").build();
        List<Map<String, Object>> mapList = client.executeQuery(source, queryDTO);
    }

    @Test
    public void executeSqlWithoutResultSet() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.DB2.getVal());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("select * from nanqi").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
    }

    @Test
    public void getTableList() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.DB2.getVal());
        List<String> tableList = client.getTableList(source, null);
        System.out.println(tableList);
    }

    @Test
    public void getTableListBySchema() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.DB2.getVal());
        List<String> tableList = client.getTableListBySchema(source, SqlQueryDTO.builder().schema("SHIHU").build());
        System.out.println(tableList);
    }

    @Test
    public void getColumnClassInfo() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.DB2.getVal());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("nanqi").build();
        List<String> columnClassInfo = client.getColumnClassInfo(source, queryDTO);
    }

    @Test
    public void getColumnMetaData() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.DB2.getVal());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("nanqi").build();
        List<ColumnMetaDTO> columnMetaData = client.getColumnMetaData(source, queryDTO);
    }

    @Test
    public void getDownloader() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.DB2.getVal());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("select * from nanqi").build();
        IDownloader downloader = client.getDownloader(source, queryDTO);
    }

    @Test
    public void preview() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.DB2.getVal());
        source.setSchema("SHIHU");
        List preview = client.getPreview(source, SqlQueryDTO.builder().previewNum(2).tableName("TEST_SHIHU").build());
        System.out.println(preview);
    }

    @Test
    public void getAllDatabases() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.DB2.getVal());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().build();
        System.out.println(client.getAllDatabases(source, queryDTO));
    }

    @Test
    public void getCurrentDatabase() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.DB2.getVal());
        String currentDatabase = client.getCurrentDatabase(source);
        Assert.assertNotNull(currentDatabase);
    }
}
