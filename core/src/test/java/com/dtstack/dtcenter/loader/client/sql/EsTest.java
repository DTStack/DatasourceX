package com.dtstack.dtcenter.loader.client.sql;

import com.alibaba.fastjson.JSONObject;
import com.dtstack.dtcenter.loader.cache.pool.config.PoolConfig;
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
public class EsTest {

    private static final IClient client = ClientCache.getClient(DataSourceType.ES6.getVal());

    private static final ESSourceDTO source = ESSourceDTO.builder()
            .url("172.16.100.186:9200")
            .poolConfig(new PoolConfig())
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
            throw new DtLoaderException("连接异常");
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
}
