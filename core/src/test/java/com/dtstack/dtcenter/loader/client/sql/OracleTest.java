package com.dtstack.dtcenter.loader.client.sql;

import com.dtstack.dtcenter.loader.IDownloader;
import com.dtstack.dtcenter.loader.client.ClientCache;
import com.dtstack.dtcenter.loader.client.IClient;
import com.dtstack.dtcenter.loader.dto.ColumnMetaDTO;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.source.OracleSourceDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import org.apache.commons.collections.CollectionUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
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
    private static OracleSourceDTO source = OracleSourceDTO.builder()
            .url("jdbc:oracle:thin:@172.16.8.193:1521:xe")
            .username("kminer")
            .password("kminerpass")
            //.schema("KMINER")
            //.poolConfig(new PoolConfig())
            .build();

    @BeforeClass
    public static void beforeClass() throws Exception {
        /*IClient client = ClientCache.getClient(DataSourceType.Oracle.getVal());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("drop table \"nanqi\"").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("create table \"nanqi\" (id int, name VARCHAR2(50))").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("comment on table \"nanqi\" is 'table comment'").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("insert into \"nanqi\" values (1, 'nanqi')").build();
        client.executeSqlWithoutResultSet(source, queryDTO);*/
    }

    @Test
    public void getCon() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.Oracle.getVal());
        Connection con1 = client.getCon(source);
        con1.close();
    }

    @Test
    public void testCon() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.Oracle.getVal());
        Boolean isConnected = client.testCon(source);
        if (Boolean.FALSE.equals(isConnected)) {
            throw new DtLoaderException("连接异常");
        }
    }

    @Test
    public void executeQuery() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.Oracle.getVal());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("select count(1) from \"nanqi\"").build();
        List<Map<String, Object>> mapList = client.executeQuery(source, queryDTO);
        System.out.println(mapList.size());
    }

    /**
     * oracle xmlType字段预览
     * @throws Exception
     */
    @Test
    public void xmlPreview() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.Oracle.getPluginName());
        source.setSchema("JIANGBO");
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("XML_TEST").build();
        List preview = client.getPreview(source, queryDTO);
        System.out.println(preview);
    }

    @Test
    public void executeQueryAlias() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.Oracle.getVal());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("select id as tAlias from \"nanqi\"").build();
        List<Map<String, Object>> mapList = client.executeQuery(source, queryDTO);
        System.out.println(mapList);
    }

    @Test
    public void executeSqlWithoutResultSet() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.Oracle.getVal());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("select count(1) from \"nanqi\"").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
    }

    @Test
    public void getTableList() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.Oracle.getVal());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().build();
        List<String> tableList = client.getTableList(source, queryDTO);
        System.out.println(tableList);
    }

    @Test
    public void getTableListBySchema() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.Oracle.getVal());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().schema("JIANGBO").build();
        List<String> tableList = client.getTableListBySchema(source, queryDTO);
        System.out.println(tableList);
    }

    @Test
    public void getColumnClassInfo() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.Oracle.getVal());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("nanqi").build();
        List<String> columnClassInfo = client.getColumnClassInfo(source, queryDTO);
        System.out.println(columnClassInfo.size());
    }

    @Test
    public void getColumnMetaData() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.Oracle.getVal());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("nanqi").build();
        List<ColumnMetaDTO> columnMetaData = client.getColumnMetaData(source, queryDTO);
        System.out.println(columnMetaData);
    }

    @Test
    public void getTableMetaComment() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.Oracle.getVal());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("nanqi").build();
        String metaComment = client.getTableMetaComment(source, queryDTO);
        System.out.println(metaComment);
    }

    /**
     * 数据预览测试
     * @throws Exception
     */
    @Test
    public void testGetPreview() throws Exception{
        IClient client = ClientCache.getClient(DataSourceType.Oracle.getVal());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("nanqi").previewNum(1).build();
        List preview = client.getPreview(source, queryDTO);
        System.out.println(preview);
    }

    @Test
    public void testGetDownloader() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.Oracle.getVal());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("select * from \"nanqi\"").build();
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
        IClient client = ClientCache.getClient(DataSourceType.Oracle.getVal());
        List data = client.getFlinkColumnMetaData(source, SqlQueryDTO.builder().tableName("nanqi").build());
        System.out.println(data);
    }

    @Test
    public void getColumnMetaDataWithSql() throws Exception{
        IClient client = ClientCache.getClient(DataSourceType.Oracle.getVal());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("select * from \"nanqi\" ").build();

        List list = client.getColumnMetaDataWithSql(source, queryDTO);

        System.out.println(list);
    }

    @Test
    public void getAllDatabases() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.Oracle.getVal());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().build();
        System.out.println(client.getAllDatabases(source,queryDTO));
    }

    @Test
    public void getCreateTableSql() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.Oracle.getVal());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("nanqi").build();
        System.out.println(client.getCreateTableSql(source,queryDTO));
    }

    @Test
    public void getCurrentDatabase() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.Oracle.getVal());
        String currentDatabase = client.getCurrentDatabase(source);
        Assert.assertNotNull(currentDatabase);
    }

    /**
     * 模糊查询表，不指定schema，限制条数
     * @throws Exception
     */
    @Test
    public void searchTableByNameLimit () throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.Oracle.getVal());
        long oldDate = System.currentTimeMillis();
        List list = client.getTableListBySchema(source, SqlQueryDTO.builder().view(true).tableNamePattern("test_vie").limit(5).build());
        Assert.assertTrue(CollectionUtils.isNotEmpty(list));
        long newDate = System.currentTimeMillis();
        System.out.println("------用时：" + (newDate - oldDate) / 1000 + "s");
    }

    /**
     * 模糊查询表，不指定schema，不获取视图，限制条数
     * @throws Exception
     */
    @Test
    public void searchTableByNameLimitNoView () throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.Oracle.getVal());
        long oldDate = System.currentTimeMillis();
        List list = client.getTableListBySchema(source, SqlQueryDTO.builder().view(false).tableNamePattern(" ").limit(100).build());
        Assert.assertTrue(CollectionUtils.isNotEmpty(list));
        long newDate = System.currentTimeMillis();
        System.out.println("------用时：" + (newDate - oldDate) / 1000 + "s");
    }

    /**
     * 获取指定schema下的表，模糊查询，限制条数
     * @throws Exception
     */
    @Test
    public void searchTableBySchemaLimit () throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.Oracle.getVal());
        long oldDate = System.currentTimeMillis();
        List list = client.getTableListBySchema(source, SqlQueryDTO.builder().view(true).schema("JIANGBO").tableNamePattern("s").limit(3).build());
        Assert.assertTrue(CollectionUtils.isNotEmpty(list));
        long newDate = System.currentTimeMillis();
        System.out.println("------用时：" + (newDate - oldDate) / 1000 + "s");
    }

    /**
     * 获取指定schema下的表，模糊查询，不限制条数
     * @throws Exception
     */
    @Test
    public void searchTableBySchema () throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.Oracle.getVal());
        long oldDate = System.currentTimeMillis();
        List list = client.getTableListBySchema(source, SqlQueryDTO.builder().view(true).schema("JIANGBO").tableNamePattern("s").build());
        Assert.assertTrue(CollectionUtils.isNotEmpty(list));
        long newDate = System.currentTimeMillis();
        System.out.println("------用时：" + (newDate - oldDate) / 1000 + "s");
    }

    /**
     * 获取指定schema下的表，模糊查询，查询条件为空字符，不限制条数
     * @throws Exception
     */
    @Test
    public void searchTableBySchemaEmptyName () throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.Oracle.getVal());
        long oldDate = System.currentTimeMillis();
        List list = client.getTableListBySchema(source, SqlQueryDTO.builder().view(true).schema("JIANGBO").tableNamePattern(" ").build());
        Assert.assertTrue(CollectionUtils.isNotEmpty(list));
        long newDate = System.currentTimeMillis();
        System.out.println("------用时：" + (newDate - oldDate) / 1000 + "s");
    }

    /**
     * 获取指定schema下的表，不获取视图，限制条数
     * @throws Exception
     */
    @Test
    public void searchTableAndViewBySchema () throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.Oracle.getVal());
        long oldDate = System.currentTimeMillis();
        List list = client.getTableListBySchema(source, SqlQueryDTO.builder().view(false).schema("SYS").tableNamePattern("test_view").limit(200).build());
        Assert.assertTrue(CollectionUtils.isEmpty(list));
        long newDate = System.currentTimeMillis();
        System.out.println("------用时：" + (newDate - oldDate) / 1000 + "s");
    }
}
