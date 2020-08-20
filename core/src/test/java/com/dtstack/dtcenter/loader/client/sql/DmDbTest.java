package com.dtstack.dtcenter.loader.client.sql;

import com.dtstack.dtcenter.loader.IDownloader;
import com.dtstack.dtcenter.loader.client.AbsClientCache;
import com.dtstack.dtcenter.loader.client.IClient;
import com.dtstack.dtcenter.loader.dto.ColumnMetaDTO;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.source.DmSourceDTO;
import com.dtstack.dtcenter.loader.enums.ClientType;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import org.junit.Test;

import java.sql.Connection;
import java.util.List;
import java.util.Map;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 13:49 2020/4/17
 * @Description：达梦数据源测试
 */
public class DmDbTest {
    private static final AbsClientCache clientCache = ClientType.DATA_SOURCE_CLIENT.getClientCache();

    DmSourceDTO source = DmSourceDTO.builder()
            .url("jdbc:dm://172.16.8.178:5236/chener")
            .username("chener")
            .password("abc123456")
            .build();

    @Test
    public void getCon() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.DMDB.getPluginName());
        Connection con = client.getCon(source);
        con.createStatement().close();
        con.close();
    }

    @Test
    public void testCon() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.DMDB.getPluginName());
        Boolean isConnected = client.testCon(source);
        if (Boolean.FALSE.equals(isConnected)) {
            throw new DtLoaderException("连接异常");
        }
    }

    @Test
    public void executeQuery() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.DMDB.getPluginName());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("select * from XQ_TEST limit 8").build();
        List<Map<String, Object>> mapList = client.executeQuery(source, queryDTO);
        System.out.println(mapList);
    }

    @Test
    public void executeSqlWithoutResultSet() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.DMDB.getPluginName());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("select * from XQ_TEST").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
    }

    @Test
    public void getTableList() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.DMDB.getPluginName());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().build();
        List<String> tableList = client.getTableList(source, queryDTO);
        System.out.println(tableList);
    }

    @Test
    public void getColumnClassInfo() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.DMDB.getPluginName());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("XQ_TEST").build();
        List<String> columnClassInfo = client.getColumnClassInfo(source, queryDTO);
        System.out.println(columnClassInfo.size());
    }

    @Test
    public void getColumnMetaData() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.DMDB.getPluginName());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("XQ_TEST").build();
        List<ColumnMetaDTO> columnMetaData = client.getColumnMetaData(source, queryDTO);
        System.out.println(columnMetaData.size());
    }

    @Test
    public void getTableMetaComment() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.DMDB.getPluginName());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("XQ_TEST").build();
        String metaComment = client.getTableMetaComment(source, queryDTO);
        System.out.println(metaComment);
    }

    @Test
    public void getDownloader() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.DMDB.getPluginName());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("select * from XQ_TEST").build();
        IDownloader downloader = client.getDownloader(source, queryDTO);
        for (int j = 0; j < 5; j++) {
            if (!downloader.reachedEnd()){
                List<List<String>> o = (List<List<String>>)downloader.readNext();
                System.out.println("=================="+j+"==================");
                for (List<String> list:o){
                    System.out.println(list);
                }
            }
        }
    }

    @Test
    public void getAllDatabases() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.DMDB.getPluginName());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().build();
        System.out.println(client.getAllDatabases(source, queryDTO));
    }

    @Test
    public void getCreateTableSql() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.DMDB.getPluginName());
        source.setSchema("chener");
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("XQ_TEST").build();
        System.out.println(client.getCreateTableSql(source, queryDTO));
    }

}
