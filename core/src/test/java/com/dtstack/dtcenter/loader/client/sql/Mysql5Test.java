package com.dtstack.dtcenter.loader.client.sql;

import com.dtstack.dtcenter.loader.IDownloader;
import com.dtstack.dtcenter.loader.cache.connection.CacheConnectionHelper;
import com.dtstack.dtcenter.loader.cache.pool.config.PoolConfig;
import com.dtstack.dtcenter.loader.client.ClientCache;
import com.dtstack.dtcenter.loader.client.IClient;
import com.dtstack.dtcenter.loader.dto.ColumnMetaDTO;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.source.Mysql5SourceDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 03:41 2020/2/29
 * @Description：MySQL 5 测试
 */
public class Mysql5Test {
    private static Mysql5SourceDTO source = Mysql5SourceDTO.builder()
            .url("jdbc:mysql://172.16.101.249:3306/streamapp")
            .username("drpeco")
            .password("DT@Stack#123")
            .schema("streamapp")
            .poolConfig(new PoolConfig())
            .build();

    @BeforeClass
    public static void beforeClass() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.MySQL.getVal());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("drop table if exists nanqi").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("create table nanqi (id int COMMENT 'id', name varchar(50) COMMENT '姓名') comment 'table comment'").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("insert into nanqi values (1, 'nanqi')").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
    }

    /**
     * 获取连接测试 - 使用连接池，默认最大开启十个连接
     * @throws Exception
     */
    @Test
    public void getCon() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.MySQL.getVal());
        Connection con1 = client.getCon(source);
        con1.close();
    }

    @Test
    public void testCon() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.MySQL.getVal());
        Boolean isConnected = client.testCon(source);
        Assert.assertTrue(isConnected);
    }

    @Test(expected = DtLoaderException.class)
    public void testErrorCon() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.MySQL.getVal());
        source.setUsername("nanqi233");
        client.testCon(source);
    }

    @Test
    public void executeQuery() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.MySQL.getVal());
        String sql = "select * from nanqi where id > ? and id < ?;";
        List<Object> preFields = new ArrayList<>();
        preFields.add(2);
        preFields.add(5);
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql(sql).preFields(preFields).queryTimeout(1).build();
        List<Map<String, Object>> mapList = client.executeQuery(source, queryDTO);
        System.out.println(mapList);
    }

    @Test
    public void executeQueryAlias() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.MySQL.getVal());
        String sql = "select id as testAlias from nanqi;";
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql(sql).build();
        List<Map<String, Object>> mapList = client.executeQuery(source, queryDTO);
        System.out.println(mapList);
    }

    @Test
    public void executeQueryCache() throws Exception {
        String sessionKey = UUID.randomUUID().toString();
        // 开启缓存
        CacheConnectionHelper.startCacheConnection(sessionKey);
        // 关闭线程池
        source.setPoolConfig(null);
        IClient client = ClientCache.getClient(DataSourceType.MySQL.getPluginName());
        String sql = "select * from rdos_dict;";
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql(sql).queryTimeout(10).build();
        client.executeQuery(source, queryDTO);
        String con1JdbcConn = source.getConnection().toString();
        client.executeQuery(source, queryDTO);
        String con2JdbcConn = source.getConnection().toString();
        // 清除缓存
        CacheConnectionHelper.closeCacheConnection(sessionKey);
        client.executeQuery(source, queryDTO);
        String con3JdbcConn = CacheConnectionHelper.getConnection(sessionKey, DataSourceType.MySQL.getVal()).toString();
        assert con1JdbcConn.equals(con2JdbcConn) && Objects.isNull(con3JdbcConn);
    }

    @Test
    public void executeSqlWithoutResultSet() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.MySQL.getVal());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("show tables").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
    }

    @Test
    public void getTableList() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.MySQL.getVal());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().build();
        List<String> tableList = client.getTableList(source, queryDTO);
        System.out.println(tableList);
    }

    @Test
    public void getTableListBySchema() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.MySQL.getVal());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().schema("ide").build();
        List<String> tableList = client.getTableListBySchema(source, queryDTO);
        System.out.println(tableList);
    }

    @Test
    public void getColumnClassInfo() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.MySQL.getVal());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("nanqi").build();
        List<String> columnClassInfo = client.getColumnClassInfo(source, queryDTO);
        System.out.println(columnClassInfo.size());
    }

    @Test
    public void getColumnMetaData() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.MySQL.getVal());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("nanqi").build();
        List<ColumnMetaDTO> columnMetaData = client.getColumnMetaData(source, queryDTO);
        System.out.println(columnMetaData);
    }

    @Test
    public void getTableMetaComment() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.MySQL.getVal());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("nanqi").build();
        String metaComment = client.getTableMetaComment(source, queryDTO);
        System.out.println(metaComment);
    }

    @Test
    public void testGetDownloader() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.MySQL.getVal());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("select * from nanqi").build();
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

    /**
     * 数据预览测试
     * @throws Exception
     */
    @Test
    public void testGetPreview() throws Exception{
        IClient client = ClientCache.getClient(DataSourceType.MySQL.getVal());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("nanqi").build();
        List preview = client.getPreview(source, queryDTO);
        System.out.println(preview);
    }

    /**
     * 根据自定义sql获取表字段信息
     */
    @Test
    public void getColumnMetaDataWithSql() throws Exception{
        IClient client = ClientCache.getClient(DataSourceType.MySQL.getVal());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("select * from nanqi").build();
        List sql = client.getColumnMetaDataWithSql(source, queryDTO);
        System.out.println(sql);
    }

    @Test
    public void getAllDatabases() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.MySQL.getVal());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().build();
        System.out.println(client.getAllDatabases(source,queryDTO));
    }

    @Test
    public void getCreateTableSql() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.MySQL.getVal());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("nanqi").build();
        System.out.println(client.getCreateTableSql(source,queryDTO));
    }

    @Test
    public void getCurrentDatabase() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.MySQL.getVal());
        String currentDatabase = client.getCurrentDatabase(source);
        Assert.assertTrue(StringUtils.isNotBlank(currentDatabase));
    }

    @Test
    public void getTableBySchema () throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.MySQL.getVal());
        List tableListBySchema = client.getTableListBySchema(source, SqlQueryDTO.builder().schema("api").tableNamePattern(" ").limit(5).build());
        Assert.assertTrue(CollectionUtils.isNotEmpty(tableListBySchema));
    }
}
