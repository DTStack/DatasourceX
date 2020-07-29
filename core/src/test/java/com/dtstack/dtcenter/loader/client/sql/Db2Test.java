package com.dtstack.dtcenter.loader.client.sql;

import com.dtstack.dtcenter.common.enums.DataSourceClientType;
import com.dtstack.dtcenter.common.exception.DtCenterDefException;
import com.dtstack.dtcenter.loader.IDownloader;
import com.dtstack.dtcenter.loader.cache.cp.CpConfig;
import com.dtstack.dtcenter.loader.client.AbsClientCache;
import com.dtstack.dtcenter.loader.client.IClient;
import com.dtstack.dtcenter.loader.dto.ColumnMetaDTO;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.source.Db2SourceDTO;
import com.dtstack.dtcenter.loader.enums.ClientType;
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
public class Db2Test {
    private static final AbsClientCache clientCache = ClientType.DATA_SOURCE_CLIENT.getClientCache();

    Db2SourceDTO source = Db2SourceDTO.builder()
            .url("jdbc:db2://172.16.10.251:50000/mqTest")
            .username("DB2INST1")
            .password("abc123")
            .schema("mqTest")
            .cpConfig(CpConfig.builder().build())
            .build();

    @Test
    public void getCon() throws Exception {
        IClient client = clientCache.getClient(DataSourceClientType.DB2.getPluginName());
        Connection con1 = client.getCon(source);
        String con1JdbcConn = con1.toString().split("wrapping")[1];
        Connection con2 = client.getCon(source);
        Connection con3 = client.getCon(source);
        Connection con4 = client.getCon(source);
        Connection con5 = client.getCon(source);
        Connection con6 = client.getCon(source);
        Connection con7 = client.getCon(source);
        Connection con8 = client.getCon(source);
        Connection con9 = client.getCon(source);
        Connection con10 = client.getCon(source);
        con1.close();
        Connection con11 = client.getCon(source);
        String con11JdbcConn = con11.toString().split("wrapping")[1];
        assert con1JdbcConn.equals(con11JdbcConn);
        con2.close();
        con3.close();
        con4.close();
        con5.close();
        con6.close();
        con7.close();
        con8.close();
        con9.close();
        con10.close();
        con11.close();
    }

    @Test
    public void testCon() throws Exception {
        IClient client = clientCache.getClient(DataSourceClientType.DB2.getPluginName());
        Boolean isConnected = client.testCon(source);
        if (Boolean.FALSE.equals(isConnected)) {
            throw new DtCenterDefException("连接异常");
        }
    }

    @Test
    public void executeQuery() throws Exception {
        IClient client = clientCache.getClient(DataSourceClientType.DB2.getPluginName());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("select * from nanqi01 limit 1,1").build();
        List<Map<String, Object>> mapList = client.executeQuery(source, queryDTO);
        System.out.println(mapList);
    }

    @Test
    public void executeSqlWithoutResultSet() throws Exception {
        IClient client = clientCache.getClient(DataSourceClientType.DB2.getPluginName());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("select * from nanqi01").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
    }

    @Test
    public void getTableList() throws Exception {
        IClient client = clientCache.getClient(DataSourceClientType.DB2.getPluginName());
        List<String> tableList = client.getTableList(source, null);
        System.out.println(tableList.size());
    }

    @Test
    public void getColumnClassInfo() throws Exception {
        IClient client = clientCache.getClient(DataSourceClientType.DB2.getPluginName());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("nanqi01").build();
        List<String> columnClassInfo = client.getColumnClassInfo(source, queryDTO);
        System.out.println(columnClassInfo.size());
    }

    @Test
    public void getColumnMetaData() throws Exception {
        IClient client = clientCache.getClient(DataSourceClientType.DB2.getPluginName());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("nanqi01").build();
        List<ColumnMetaDTO> columnMetaData = client.getColumnMetaData(source, queryDTO);
        System.out.println(columnMetaData.size());
    }

    @Test
    public void getDownloader() throws Exception {
        IClient client = clientCache.getClient(DataSourceClientType.DB2.getPluginName());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("select * from WANGCHUAN01").build();
        IDownloader downloader = client.getDownloader(source, queryDTO);
        System.out.println(downloader.getMetaInfo());
        while (!downloader.reachedEnd()){
            List<List<String>> o = (List<List<String>>)downloader.readNext();
            for (List<String> list:o)
                System.out.println(list);
        }
    }

    @Test
    public void preview() throws Exception {
        IClient client = clientCache.getClient(DataSourceClientType.DB2.getPluginName());
        List preview = client.getPreview(source, SqlQueryDTO.builder().tableName("WANGCHUAN01").build());
        System.out.println(preview);
    }
}
