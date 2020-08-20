package com.dtstack.dtcenter.loader.client.sql;

import com.dtstack.dtcenter.loader.IDownloader;
import com.dtstack.dtcenter.loader.cache.pool.config.PoolConfig;
import com.dtstack.dtcenter.loader.client.AbsClientCache;
import com.dtstack.dtcenter.loader.client.IClient;
import com.dtstack.dtcenter.loader.dto.ColumnMetaDTO;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.source.Mysql5SourceDTO;
import com.dtstack.dtcenter.loader.enums.ClientType;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Connection;
import java.util.List;
import java.util.Map;

/**
 * @company: www.dtstack.com
 * @Author ：wangchuan
 * @Date ：Created in 上午9:42 2020/7/29
 * @Description：
 */
public class TiDBTest {

    private static final AbsClientCache clientCache = ClientType.DATA_SOURCE_CLIENT.getClientCache();
    Mysql5SourceDTO source = Mysql5SourceDTO.builder()
            .url("jdbc:mysql://121.41.87.224:4000/test")
            .username("test")
            .password("abc123")
            .poolConfig(new PoolConfig())
            .build();

    /**
     * 获取连接测试 - 使用连接池，默认最大开启十个连接
     * @throws Exception
     */
    @Test
    public void getCon() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.TiDB.getPluginName());
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
        IClient client = clientCache.getClient(DataSourceType.TiDB.getPluginName());
        Boolean isConnected = client.testCon(source);
        Assert.assertTrue(isConnected);
    }

    @Test(expected = DtLoaderException.class)
    public void testErrorCon() {
        IClient client = clientCache.getClient(DataSourceType.TiDB.getPluginName());
        source.setUsername("wangchuan");
        client.testCon(source);
    }

    @Test
    public void executeQuery() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.TiDB.getPluginName());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("show tables").build();
        List<Map<String, Object>> mapList = client.executeQuery(source, queryDTO);
        System.out.println(mapList.size());
    }

    @Test
    public void executeSqlWithoutResultSet() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.TiDB.getPluginName());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("show tables").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
    }

    @Test
    public void getTableList() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.TiDB.getPluginName());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().build();
        List<String> tableList = client.getTableList(source, queryDTO);
        System.out.println(tableList);
    }

    @Test
    public void getColumnClassInfo() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.TiDB.getPluginName());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("demo").build();
        List<String> columnClassInfo = client.getColumnClassInfo(source, queryDTO);
        System.out.println(columnClassInfo.size());
    }

    @Test
    public void getColumnMetaData() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.TiDB.getPluginName());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("demo").build();
        List<ColumnMetaDTO> columnMetaData = client.getColumnMetaData(source, queryDTO);
        System.out.println(columnMetaData.size());
    }

    @Test
    public void getTableMetaComment() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.TiDB.getPluginName());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("demo").build();
        String metaComment = client.getTableMetaComment(source, queryDTO);
        System.out.println(metaComment);
    }

    @Test
    public void testGetDownloader() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.TiDB.getPluginName());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("select * from demo").build();
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
        downloader.close();
    }

    /**
     * 数据预览测试
     * @throws Exception
     */
    @Test
    public void testGetPreview() throws Exception{
        IClient client = clientCache.getClient(DataSourceType.TiDB.getPluginName());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("demo").build();
        List preview = client.getPreview(source, queryDTO);
        System.out.println(preview);
    }

}
