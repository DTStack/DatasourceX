package com.dtstack.dtcenter.loader.client.sql;

import com.alibaba.fastjson.JSONObject;
import com.dtstack.dtcenter.loader.cache.pool.config.PoolConfig;
import com.dtstack.dtcenter.loader.client.BaseTest;
import com.dtstack.dtcenter.loader.client.ClientCache;
import com.dtstack.dtcenter.loader.client.IClient;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.source.ESSourceDTO;
import com.dtstack.dtcenter.loader.enums.EsCommandType;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import org.apache.commons.collections.CollectionUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;
import java.util.Map;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 23:09 2020/2/28
 * @Description：ES 测试
 */
public class EsTest extends BaseTest {

    private static final IClient client = ClientCache.getClient(DataSourceType.ES6.getVal());

    private static final ESSourceDTO source = ESSourceDTO.builder()
            .url("172.16.100.186:9200")
            .poolConfig(new PoolConfig())
            .build();

    /**
     * 用户名和密码不正确，172.16.100.186:9200 不需要密码
     */
    private static final ESSourceDTO esSource = ESSourceDTO.builder()
            .url("172.16.100.186:9200")
            .password("abc")
            .username("123")
            .build();

    /**
     * 数据准备
     */
    @BeforeClass
    public static void setUp () {
        String sql = "{\"name\": \"小黄\", \"age\": 18,\"sex\": \"不详\",\"extraAttr_0_5_3\":{\"attributeValue\":\"2020-09-17 23:37:16\"}}";
        String tableName = "commodity/_doc/3";
        client.executeSqlWithoutResultSet(source, SqlQueryDTO.builder().sql(sql).tableName(tableName).esCommandType(EsCommandType.INSERT.getType()).build());
    }

    @Test
    public void testCon() throws Exception {
        Boolean isConnected = client.testCon(source);
        if (Boolean.FALSE.equals(isConnected)) {
            throw new DtLoaderException("connection exception");
        }
    }

    @Test
    public void getAllDb () {
        List databases = client.getAllDatabases(source, SqlQueryDTO.builder().build());
        Assert.assertTrue(CollectionUtils.isNotEmpty(databases));
    }

    @Test
    public void getTableList() {
        List tableList = client.getTableList(source, SqlQueryDTO.builder().tableName("commodity").build());
        Assert.assertTrue(CollectionUtils.isNotEmpty(tableList));
    }

    @Test
    public void getPreview() {
        List viewList = client.getPreview(source, SqlQueryDTO.builder().tableName("commodity").previewNum(5).build());
        Assert.assertTrue(CollectionUtils.isNotEmpty(viewList));
    }

    @Test
    public void getColumnMetaData() {
        List metaData = client.getColumnMetaData(source, SqlQueryDTO.builder().tableName("commodity").build());
        Assert.assertTrue(CollectionUtils.isNotEmpty(metaData));
    }

    @Test
    public void executeQuery() {
        List<Map<String, Object>> list = client.executeQuery(source, SqlQueryDTO.builder().sql("{\"query\": {\"match_all\": {} }}").tableName("commodity").build());
        JSONObject result = (JSONObject) list.get(0).get("result");
        Assert.assertNotNull(result);
    }

    /**
     * 删除
     */
    @Test
    public void executeSqlWithoutResultSet() {
        IClient client = ClientCache.getClient(DataSourceType.ES6.getVal());
        String tableName = "commodity/_doc/3";
        client.executeSqlWithoutResultSet(source, SqlQueryDTO.builder().tableName(tableName).esCommandType(EsCommandType.DELETE.getType()).build());
    }

    @Test
    public void executeSqlWithoutResultSet4() {
        String sql = "{\"doc\":{\"age\":26 }}";
        String tableName = "commodity/_doc/3";
        client.executeSqlWithoutResultSet(source, SqlQueryDTO.builder().sql(sql).tableName(tableName).esCommandType(EsCommandType.UPDATE.getType()).build());
    }

    /**
     * 插数据测试
     */
    @Test
    public void executeSqlWithoutResultSet3() {
        String sql = "{\"name\": \"小黄\", \"age\": 18,\"sex\": \"不详\",\"extraAttr_0_5_3\":{\"attributeValue\":\"2020-09-17 23:37:16\"}}";
        String tableName = "commodity/_doc/3";
        client.executeSqlWithoutResultSet(source, SqlQueryDTO.builder().sql(sql).tableName(tableName).esCommandType(EsCommandType.INSERT.getType()).build());
    }

    /**
     * 根据查询更新
     */
    @Test
    public void executeSqlWithoutResultSet2() {
        String sql = "{\"query\": {\"match_all\": {} }}";
        String tableName = "commodity/_doc";
        client.executeSqlWithoutResultSet(source, SqlQueryDTO.builder().sql(sql).tableName(tableName).esCommandType(EsCommandType.UPDATE_BY_QUERY.getType()).build());
    }

    /**
     * 根据查询删除
     */
    @Test
    public void executeSqlWithoutResultSet1() {
        String sql = "{\"query\":{\"match_all\": {}}}";
        String tableName = "commodity/_doc";
        client.executeSqlWithoutResultSet(source, SqlQueryDTO.builder().sql(sql).tableName(tableName).esCommandType(EsCommandType.DELETE_BY_QUERY.getType()).build());
    }

    /**
     * 连接失败
     */
    @Test
    public void testConFalse() {
        ESSourceDTO source = new ESSourceDTO();
        Boolean isConnected = client.testCon(source);
        Assert.assertFalse(isConnected);
    }

    /**
     * 获取表失败
     */
    @Test(expected = DtLoaderException.class)
    public void getTableListFalse() {
        ESSourceDTO source1 = new ESSourceDTO();
        List<String> list = client.getTableList(source1, null);
        assert CollectionUtils.isEmpty(list);
        client.getTableList(source, SqlQueryDTO.builder().build());
    }

    /**
     * 获取ES所有索引,校验null
     */
    @Test
    public void getAllDatabasesFalse() {
        ESSourceDTO source1 = new ESSourceDTO();
        List<String> list1= client.getAllDatabases(source1, null);
        assert CollectionUtils.isEmpty(list1);
        client.getAllDatabases(source1, SqlQueryDTO.builder().build());
    }

    /**
     * 数据预览，检验null
     */
    @Test(expected = DtLoaderException.class)
    public void getPreviewFalse() {
        ESSourceDTO source1 = new ESSourceDTO();
        List<List<Object>> list1= client.getPreview(source1, null);
        assert CollectionUtils.isEmpty(list1);
        client.getPreview(source, SqlQueryDTO.builder().build());
    }

    /**
     * 获取ES字段信息 ，校验null
     */
    @Test(expected = DtLoaderException.class)
    public void getColumnMetaDataReturnFalse(){
        ESSourceDTO source1 = new ESSourceDTO();
        List<List<Object>> list1= client.getColumnMetaData(source1, null);
        assert CollectionUtils.isEmpty(list1);
        client.getColumnMetaData(source, SqlQueryDTO.builder().build());
    }

    /**
     * 执行query，校验null
     */
    @Test(expected = DtLoaderException.class)
    public void executeQueryReturnFalse(){
        ESSourceDTO source1 = new ESSourceDTO();
        List<List<Object>> list1= client.executeQuery(source1, null);
        assert CollectionUtils.isEmpty(list1);
        client.executeQuery(source, SqlQueryDTO.builder().build());
    }

    /**
     * 测试链接，使用用户名密码
     */
    @Test
    public void testConUseName() {
        Boolean isConnected = client.testCon(esSource);
        Assert.assertTrue(isConnected);
        Boolean isConnect = client.testCon(esSource);
        Assert.assertTrue(isConnect);
    }

    @Test(expected = DtLoaderException.class)
    public void getColumnClassInfo() {
        client.getColumnClassInfo(esSource, null);
    }

    @Test(expected = DtLoaderException.class)
    public void getColumnMetaDataWithSql() {
        client.getColumnMetaDataWithSql(esSource, null);
    }

    @Test(expected = DtLoaderException.class)
    public void getCreateTableSql() {
        client.getCreateTableSql(esSource, null);
    }


    @Test(expected = DtLoaderException.class)
    public void getPartitionColumn() {
        client.getPartitionColumn(esSource, null);
    }

    @Test(expected = DtLoaderException.class)
    public void getDownloader() throws Exception {
        client.getDownloader(esSource, null);
    }

    @Test(expected = DtLoaderException.class)
    public void getFlinkColumnMetaData() {
        client.getFlinkColumnMetaData(esSource, null);
    }

    @Test(expected = DtLoaderException.class)
    public void getTableMetaComment() {
        client.getTableMetaComment(esSource, null);
    }

    @Test(expected = DtLoaderException.class)
    public void getTable() {
        client.getTable(esSource, null);
    }

    @Test(expected = DtLoaderException.class)
    public void getCurrentDatabase() {
        client.getCurrentDatabase(esSource);
    }

    @Test(expected = DtLoaderException.class)
    public void createDatabase() {
        client.createDatabase(esSource, null ,null);
    }

    @Test(expected = DtLoaderException.class)
    public void isDatabaseExists() {
        client.isDatabaseExists(esSource, null);
    }



    @Test(expected = DtLoaderException.class)
    public void isTableExistsInDatabase() {
        client.isTableExistsInDatabase(esSource, null, null);
    }


}
