package com.dtstack.dtcenter.loader.client.sql;

import com.alibaba.fastjson.JSONObject;
import com.dtstack.dtcenter.loader.cache.pool.config.PoolConfig;
import com.dtstack.dtcenter.loader.client.AbsClientCache;
import com.dtstack.dtcenter.loader.client.IClient;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.source.ESSourceDTO;
import com.dtstack.dtcenter.loader.enums.ClientType;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.dtstack.dtcenter.loader.source.DataSourceType;
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
    private static final AbsClientCache clientCache = ClientType.DATA_SOURCE_CLIENT.getClientCache();

    ESSourceDTO source = ESSourceDTO.builder()
            .url("172.16.8.193:9201,172.16.8.193:9202,172.16.8.193:9203")
            //.username("elastic")
            //.password("abc123")
            //.schema("tools")
            //.id("id_1")
            //.isCache(true)
            .poolConfig(new PoolConfig())
            .build();

    @Test
    public void testCon() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.ES6.getPluginName());
        Boolean isConnected = client.testCon(source);
        if (Boolean.FALSE.equals(isConnected)) {
            throw new DtLoaderException("连接异常");
        }
    }

    @Test
    public void getTableList() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.ES6.getPluginName());
        List tableList = client.getTableList(source, SqlQueryDTO.builder().tableName("tools").build());
        System.out.println(tableList);
    }

    @Test
    public void getPreview() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.ES6.getPluginName());
        List viewList = client.getPreview(source, SqlQueryDTO.builder().tableName("tools").previewNum(5).build());
        System.out.println(viewList);
    }

    @Test
    public void getColumnMetaData() throws Exception{
        IClient client = clientCache.getClient(DataSourceType.ES6.getPluginName());
        List metaData = client.getColumnMetaData(source, SqlQueryDTO.builder().tableName("tools").build());
        System.out.println(metaData);
    }

    @Test
    public void getDB() throws Exception{
        IClient client = clientCache.getClient(DataSourceType.ES6.getPluginName());
        List list = client.getAllDatabases(source, SqlQueryDTO.builder().build());
        System.out.println(list);
    }

    @Test
    public void executeQuery() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.ES6.getPluginName());
        List<Map<String, Object>> list = client.executeQuery(source, SqlQueryDTO.builder().sql("{\"query\": {\"match_all\": {}    }}").tableName("tools").build());
        JSONObject result = (JSONObject) list.get(0).get("result");
        System.out.println(result.toJSONString());
    }
}
