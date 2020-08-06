package com.dtstack.dtcenter.loader.client.sql;

import com.dtstack.dtcenter.common.exception.DtCenterDefException;
import com.dtstack.dtcenter.loader.IDownloader;
import com.dtstack.dtcenter.loader.client.AbsClientCache;
import com.dtstack.dtcenter.loader.client.IClient;
import com.dtstack.dtcenter.loader.dto.ColumnMetaDTO;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.source.OracleSourceDTO;
import com.dtstack.dtcenter.loader.enums.ClientType;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import org.junit.Test;

import java.sql.Connection;
import java.util.List;
import java.util.Map;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 03:54 2020/2/29
 * @Description：Oracle 测试
 */
public class OracleTest {
    private static final AbsClientCache clientCache = ClientType.DATA_SOURCE_CLIENT.getClientCache();

    OracleSourceDTO source = OracleSourceDTO.builder()
            .url("jdbc:oracle:thin:@172.16.8.193:1521:orcl")
            .username("system")
            .password("oracle")
            .schema("MDSYS")
            .build();

    @Test
    public void getCon() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.Oracle.getPluginName());
        Connection con = client.getCon(source);
        con.createStatement().close();
        con.close();
    }

    @Test
    public void testCon() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.Oracle.getPluginName());
        Boolean isConnected = client.testCon(source);
        if (Boolean.FALSE.equals(isConnected)) {
            throw new DtCenterDefException("连接异常");
        }
    }

    @Test
    public void executeQuery() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.Oracle.getPluginName());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("select count(1) from MUYUN1226").build();
        List<Map<String, Object>> mapList = client.executeQuery(source, queryDTO);
        System.out.println(mapList.size());
    }

    @Test
    public void executeSqlWithoutResultSet() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.Oracle.getPluginName());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("select count(1) from MUYUN1226").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
    }

    @Test
    public void getTableList() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.Oracle.getPluginName());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().build();
        List<String> tableList = client.getTableList(source, queryDTO);
        System.out.println(tableList);
    }

    @Test
    public void getColumnClassInfo() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.Oracle.getPluginName());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("sidetest").build();
        List<String> columnClassInfo = client.getColumnClassInfo(source, queryDTO);
        System.out.println(columnClassInfo.size());
    }

    @Test
    public void getColumnMetaData() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.Oracle.getPluginName());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("MVIEW$_ADV_TEMP").build();
        List<ColumnMetaDTO> columnMetaData = client.getColumnMetaData(source, queryDTO);
        System.out.println(columnMetaData);
    }

    @Test
    public void getTableMetaComment() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.Oracle.getPluginName());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("test_side").build();
        String metaComment = client.getTableMetaComment(source, queryDTO);
        System.out.println(metaComment);
    }

    /**
     * 数据预览测试
     * @throws Exception
     */
    @Test
    public void testGetPreview() throws Exception{
        IClient client = clientCache.getClient(DataSourceType.Oracle.getPluginName());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("MVIEW$_ADV_TEMP").previewNum(1).build();
        List preview = client.getPreview(source, queryDTO);
        System.out.println(preview);
    }

    @Test
    public void testGetDownloader() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.Oracle.getPluginName());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("select * from MVIEW$_ADV_TEMP").build();
        IDownloader downloader = client.getDownloader(source, queryDTO);
        downloader.configure();
        List<String> metaInfo = downloader.getMetaInfo();
        System.out.println(metaInfo);
        while (!downloader.reachedEnd()){
            List<List<String>> o = (List<List<String>>)downloader.readNext();
            for (List<String> list:o){
                System.out.println(list);
            }
        }
    }

    @Test
    public void getFlinkColumnMetaData() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.Oracle.getPluginName());
        List data = client.getFlinkColumnMetaData(source, SqlQueryDTO.builder().tableName("JIANGBO.TB1").build());
        System.out.println(data);
    }

    @Test
    public void getColumnMetaDataWithSql() throws Exception{
        IClient client = clientCache.getClient(DataSourceType.Oracle.getPluginName());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("select * from MVIEW$_ADV_TEMP left join DEF$_ORIGIN on 1=1 ").build();

        List list = client.getColumnMetaDataWithSql(source, queryDTO);

        System.out.println(list);
    }

    @Test
    public void getAllDatabases() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.Oracle.getPluginName());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().build();
        System.out.println(client.getAllDatabases(source,queryDTO));
    }

    @Test
    public void getCreateTableSql() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.Oracle.getPluginName());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("SDO_CS_SRS").build();
        System.out.println(client.getCreateTableSql(source,queryDTO));
    }

}
