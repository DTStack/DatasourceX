package com.dtstack.dtcenter.loader.client.sql;

import com.dtstack.dtcenter.loader.cache.pool.config.PoolConfig;
import com.dtstack.dtcenter.loader.client.AbsClientCache;
import com.dtstack.dtcenter.loader.client.IClient;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.source.MongoSourceDTO;
import com.dtstack.dtcenter.loader.enums.ClientType;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import org.apache.commons.math3.util.Pair;
import org.junit.Test;

import java.util.List;
import java.util.Map;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 14:07 2020/2/29
 * @Description：Mongo 测试
 */
public class MongoTest {
    private static final AbsClientCache clientCache = ClientType.DATA_SOURCE_CLIENT.getClientCache();

    MongoSourceDTO source = MongoSourceDTO.builder()
            .hostPort("172.16.8.193:27017/dtstack")
            .poolConfig(new PoolConfig())
            .build();

    @Test
    public void testCon() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.MONGODB.getPluginName());
        Boolean isConnected = client.testCon(source);
        if (Boolean.FALSE.equals(isConnected)) {
            throw new DtLoaderException("连接异常");
        }
    }

    @Test
    public void getTableList() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.MONGODB.getPluginName());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().build();
        List<String> tableList = client.getTableList(source, queryDTO);
        System.out.println(tableList);
    }

    @Test
    public void getDatabaseList() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.MONGODB.getPluginName());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().build();
        List list = client.getAllDatabases(source, queryDTO);
        System.out.println(list);
    }

    @Test
    public void getPreview() throws Exception{
        IClient client = clientCache.getClient(DataSourceType.MONGODB.getPluginName());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("user").build();
        List<List<Object>> preview = client.getPreview(source, queryDTO);
        preview.forEach(list->{
            list.forEach(pair->{
                Pair p = (Pair)pair;
                System.out.println(p.getKey()+"   "+p.getValue());
            });
        });

    }

    @Test
    public void executorQuery() throws Exception {
        IClient<List> client = clientCache.getClient(DataSourceType.MONGODB.getPluginName());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("db.user.find({});").startRow(1).limit(5).build();
        List<Map<String, Object>> result = client.executeQuery(source, queryDTO);
        result.forEach(map->{
            map.keySet().forEach(x->{
                System.out.println(x+"==="+map.get(x));
            });
        });
    }
}
