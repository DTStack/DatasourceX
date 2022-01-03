package com.dtstack.dtcenter.loader.client.sql;

import com.dtstack.dtcenter.loader.IDownloader;
import com.dtstack.dtcenter.loader.cache.pool.config.PoolConfig;
import com.dtstack.dtcenter.loader.client.BaseTest;
import com.dtstack.dtcenter.loader.client.ClientCache;
import com.dtstack.dtcenter.loader.client.IClient;
import com.dtstack.dtcenter.loader.dto.ColumnMetaDTO;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.source.SapHana1SourceDTO;
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

/**
 * sap hana 单元测试
 *
 * @author ：wangchuan
 * date：Created in 上午10:13 2021/12/31
 * company: www.dtstack.com
 */
public class SapHana5Test extends BaseTest {

    // 获取数据源 client
    private static final IClient client = ClientCache.getClient(DataSourceType.SAP_HANA1.getVal());

    // 构建数据源信息
    private static final SapHana1SourceDTO source = SapHana1SourceDTO.builder()
            .url("jdbc:sap://172.16.22.151:39015/HDB90")
            .username("SYSTEM")
            .password("Abc!@#579")
            .schema("SYSTEM")
            .poolConfig(PoolConfig.builder().build())
            .build();

    /**
     * 数据预处理
     */
    @BeforeClass
    public static void beforeClass() {
        try {
            client.createDatabase(source, "LOADER_TEST_SCHEMA", null);
        } catch (Exception e) {
            // ignore error
        }
        source.setSchema("LOADER_TEST_SCHEMA");
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("drop table LOADER_TEST").build();
        try {
            client.executeSqlWithoutResultSet(source, queryDTO);
        } catch (Exception e) {
            // ignore error
        }
        queryDTO = SqlQueryDTO.builder().sql("create table LOADER_TEST (id int COMMENT 'id', name varchar(50) COMMENT '姓名') comment 'table comment'").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("insert into LOADER_TEST values (1, 'LOADER_TEST')").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("drop view LOADER_TEST_VIEW").build();
        try {
            client.executeSqlWithoutResultSet(source, queryDTO);
        } catch (Exception e) {
            // ignore error
        }
        queryDTO = SqlQueryDTO.builder().sql("create view LOADER_TEST_VIEW as select * from LOADER_TEST").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
    }

    /**
     * 根据schema获取表
     */
    @Test
    public void getTableListBySchemaWithSchema() {
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().schema("LOADER_TEST_SCHEMA").build();
        List<String> tableList = client.getTableListBySchema(source, queryDTO);
        Assert.assertTrue(CollectionUtils.isNotEmpty(tableList));
    }

    /**
     * 未指定 schema
     */
    @Test
    public void getTableListNotSelectSchema() {
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().build();
        List<String> tableList = client.getTableList(source, queryDTO);
        Assert.assertTrue(CollectionUtils.isNotEmpty(tableList));
    }

    @Test
    public void getColumnMetaData() {
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().schema("LOADER_TEST_SCHEMA").tableName("LOADER_TEST").build();
        List<ColumnMetaDTO> columnMetaData = client.getColumnMetaData(source, queryDTO);
        Assert.assertTrue(CollectionUtils.isNotEmpty(columnMetaData));
    }


    /**
     * 获取连接测试
     */
    @Test
    public void getCon() throws Exception{
        Connection connection = client.getCon(source);
        Assert.assertNotNull(connection);
        connection.close();
    }

    /**
     * 连通性测试
     */
    @Test
    public void testCon() {
        Boolean isConnected = client.testCon(source);
        Assert.assertTrue(isConnected);
    }

    /**
     * 预编译查询
     */
    @Test
    public void executeQuery() {
        String sql = "select * from LOADER_TEST where id > ? and id < ?;";
        List<Object> preFields = new ArrayList<>();
        preFields.add(0);
        preFields.add(5);
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql(sql).preFields(preFields).build();
        List<Map<String, Object>> result = client.executeQuery(source, queryDTO);
        Assert.assertTrue(CollectionUtils.isNotEmpty(result));
    }

    /**
     * 字段名称重复测试
     */
    @Test
    public void executeQueryRepeatColumn() {
        String sql = "select id, name ,id as id, name, name as name, id as name,name as id from LOADER_TEST";
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql(sql).build();
        List<Map<String, Object>> result = client.executeQuery(source, queryDTO);
        Map<String, Object> row = result.get(0);
        Assert.assertEquals(7, row.size());
        Assert.assertTrue(row.containsKey("NAME") && row.containsKey("NAME(1)")
                && row.containsKey("NAME(2)") && row.containsKey("NAME(3)")
                && row.containsKey("ID") && row.containsKey("ID(1)")
                && row.containsKey("ID(2)"));
    }

    /**
     * 字段别名测试
     */
    @Test
    public void executeQueryAlias() {
        String sql = "SELECT id AS TEST_ALIAS from LOADER_TEST;";
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql(sql).build();
        List<Map<String, Object>> result = client.executeQuery(source, queryDTO);
        Assert.assertTrue(result.get(0).containsKey("TEST_ALIAS"));
    }

    /**
     * 无需结果查询
     */
    @Test
    public void executeSqlWithoutResultSet() {
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("SELECT CURRENT_SCHEMA FROM DUMMY").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
    }

    /**
     * 获取表列表
     */
    @Test
    public void getTableList() {
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().build();
        List<String> tableList = client.getTableList(source, queryDTO);
        Assert.assertTrue(CollectionUtils.isNotEmpty(tableList));
    }

    /**
     * 获取表字段 java 标准格式
     */
    @Test
    public void getColumnClassInfo() {
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("LOADER_TEST").build();
        List<String> columnClassInfo = client.getColumnClassInfo(source, queryDTO);
        Assert.assertTrue(CollectionUtils.isNotEmpty(columnClassInfo));
    }

    /**
     * 获取表注释
     */
    @Test
    public void getTableMetaComment() {
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("LOADER_TEST").build();
        String metaComment = client.getTableMetaComment(source, queryDTO);
        Assert.assertTrue(StringUtils.isNotBlank(metaComment));
    }

    /**
     * 自定义sql 数据下载测试
     */
    @Test
    public void testGetDownloader() throws Exception {
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("select * from LOADER_TEST").build();
        IDownloader downloader = client.getDownloader(source, queryDTO);
        List<String> metaInfo = downloader.getMetaInfo();
        Assert.assertTrue(CollectionUtils.isNotEmpty(metaInfo));
        while (!downloader.reachedEnd()){
            List<List<String>> result = (List<List<String>>)downloader.readNext();
            for (List<String> row : result){
                Assert.assertTrue(CollectionUtils.isNotEmpty(row));
            }
        }
    }

    /**
     * 数据预览测试
     */
    @Test
    public void testGetPreview() {
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("LOADER_TEST").build();
        List preview = client.getPreview(source, queryDTO);
        Assert.assertTrue(CollectionUtils.isNotEmpty(preview));
    }

    /**
     * 根据自定义sql获取表字段信息
     */
    @Test
    public void getColumnMetaDataWithSql() {
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("select * from LOADER_TEST").build();
        List sql = client.getColumnMetaDataWithSql(source, queryDTO);
        Assert.assertTrue(CollectionUtils.isNotEmpty(sql));
    }

    /**
     * 获取所有的db
     */
    @Test
    public void getAllDatabases() {
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().build();
        List allDatabases = client.getAllDatabases(source, queryDTO);
        Assert.assertTrue(CollectionUtils.isNotEmpty(allDatabases));
    }

    /**
     * 获取正在使用的database
     */
    @Test
    public void getCurrentDatabase() {
        String currentDatabase = client.getCurrentDatabase(source);
        Assert.assertTrue(StringUtils.isNotBlank(currentDatabase));
    }

    /**
     * 根据 schema 获取表
     */
    @Test
    public void getTableBySchema () {
        List tableListBySchema = client.getTableListBySchema(source, SqlQueryDTO.builder().schema("LOADER_TEST_SCHEMA").tableNamePattern("").limit(5).build());
        Assert.assertTrue(CollectionUtils.isNotEmpty(tableListBySchema));
    }

    /**
     * 获取downloader
     */
    @Test
    public void getDownloader() throws Exception {
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("LOADER_TEST").sql("select * from LOADER_TEST").build();
        IDownloader iDownloader = client.getDownloader(source, queryDTO);
        List<String> list = iDownloader.getMetaInfo();
        Assert.assertTrue(CollectionUtils.isNotEmpty(list));
    }

    @Test
    public void isDatabaseExists() {
       assert  client.isDatabaseExists(source, "LOADER_TEST_SCHEMA");
    }
    @Test
    public void isTableExistsInDatabase() {
       assert  client.isTableExistsInDatabase(source, "LOADER_TEST","LOADER_TEST_SCHEMA");
    }

    /**
     * 获取表 - 无视图
     */
    @Test
    public void tableListNotView() {
        List tableListBySchema = client.getTableListBySchema(source, SqlQueryDTO.builder().schema("LOADER_TEST_SCHEMA").tableNamePattern("").limit(5).build());
        Assert.assertFalse(tableListBySchema.contains("LOADER_TEST_VIEW"));
    }

    /**
     * 获取表 - 有视图
     */
    @Test
    public void tableListContainView() {
        List tableListBySchema = client.getTableListBySchema(source, SqlQueryDTO.builder().schema("LOADER_TEST_SCHEMA").tableNamePattern("").view(true).limit(5).build());
        Assert.assertTrue(tableListBySchema.contains("LOADER_TEST_VIEW"));
    }
}
