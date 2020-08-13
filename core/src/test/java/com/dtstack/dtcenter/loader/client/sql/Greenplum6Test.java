package com.dtstack.dtcenter.loader.client.sql;

import com.dtstack.dtcenter.common.exception.DtCenterDefException;
import com.dtstack.dtcenter.loader.client.AbsClientCache;
import com.dtstack.dtcenter.loader.client.IClient;
import com.dtstack.dtcenter.loader.downloader.IDownloader;
import com.dtstack.dtcenter.loader.dto.ColumnMetaDTO;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.source.Greenplum6SourceDTO;
import com.dtstack.dtcenter.loader.enums.ClientType;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import org.junit.Test;

import java.sql.Connection;
import java.util.List;
import java.util.Map;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 03:41 2020/2/29
 * @Description：MySQL 5 测试
 */
public class Greenplum6Test {
    private static final AbsClientCache clientCache = ClientType.DATA_SOURCE_CLIENT.getClientCache();

    Greenplum6SourceDTO source = Greenplum6SourceDTO.builder()
            .url("jdbc:pivotal:greenplum://172.16.10.90:5432;DatabaseName=data")
            .username("gpadmin")
            .password("gpadmin")
            .schema("public")
            .build();

    @Test
    public void getCon() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.GREENPLUM6.getPluginName());
        Connection con = client.getCon(source);
        con.createStatement().close();
        con.close();
    }

    @Test
    public void testCon() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.GREENPLUM6.getPluginName());
        Boolean isConnected = client.testCon(source);
        if (Boolean.FALSE.equals(isConnected)) {
            throw new DtCenterDefException("连接异常");
        }
    }

    @Test
    public void executeQuery() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.GREENPLUM6.getPluginName());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("select * from nanqi102 limit 2 offset ").build();
        List<Map<String, Object>> mapList = client.executeQuery(source, queryDTO);
        System.out.println(mapList);
    }

    @Test
    public void executeSqlWithoutResultSet() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.GREENPLUM6.getPluginName());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("CREATE TABLE if not exists nanqi102 ( id integer )").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
    }

    @Test
    public void getTableList() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.GREENPLUM6.getPluginName());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().build();
        List<String> tableList = client.getTableList(source, queryDTO);
        System.out.println(tableList.size());
    }

    @Test
    public void getColumnClassInfo() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.GREENPLUM6.getPluginName());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("nanqi102").build();
        List<String> columnClassInfo = client.getColumnClassInfo(source, queryDTO);
        columnClassInfo.forEach(column -> {
            System.out.println(column);
        });
        System.out.println(columnClassInfo.size());
    }

    @Test
    public void getColumnMetaData() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.GREENPLUM6.getPluginName());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("student").build();
        List<ColumnMetaDTO> columnMetaData = client.getColumnMetaData(source, queryDTO);
        System.out.println(columnMetaData.size());
        columnMetaData.forEach(column -> {
            System.out.println(column);
        });
    }

    @Test
    public void getTableMetaComment() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.GREENPLUM6.getPluginName());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("student").build();
        String metaComment = client.getTableMetaComment(source, queryDTO);
        System.out.println(metaComment);
    }

    @Test
    public void getDownloader() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.GREENPLUM6.getPluginName());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("select * from nanqi102").build();
        IDownloader downloader = client.getDownloader(source, queryDTO);
        System.out.println(downloader.getMetaInfo());
        int i = 0;
        while (!downloader.reachedEnd()){
            System.out.println("==================第"+ ++i+"页==================");
            List<List<String>> o = (List<List<String>>)downloader.readNext();
            for (List list:o){
                System.out.println(list);
            }
        }
    }

    @Test
    public void testGetPreview() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.GREENPLUM6.getPluginName());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("nanqi102").previewNum(50).build();
        List preview = client.getPreview(source, queryDTO);
        System.out.println(preview);
    }

    @Test
    public void getAllDatabases() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.GREENPLUM6.getPluginName());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().build();
        System.out.println(client.getAllDatabases(source, queryDTO));
    }

}
