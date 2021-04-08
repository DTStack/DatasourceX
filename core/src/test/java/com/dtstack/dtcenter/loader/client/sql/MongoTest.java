package com.dtstack.dtcenter.loader.client.sql;

import com.dtstack.dtcenter.loader.cache.pool.config.PoolConfig;
import com.dtstack.dtcenter.loader.client.ClientCache;
import com.dtstack.dtcenter.loader.client.IClient;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.source.MongoSourceDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.math3.util.Pair;
import org.junit.Assert;
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

    // 构建client
    private static final IClient client = ClientCache.getClient(DataSourceType.MONGODB.getVal());

    // 构建数据源信息
    private static final MongoSourceDTO source = MongoSourceDTO.builder()
            .hostPort("172.16.101.246:27017/admin")
            .poolConfig(new PoolConfig())
            .build();

    /**
     * 连通性测试
     */
    @Test
    public void testCon() {
        Boolean isConnected = client.testCon(source);
        if (Boolean.FALSE.equals(isConnected)) {
            throw new DtLoaderException("connection exception");
        }
    }

    @Test
    public void getTableList() {
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().build();
        List<String> tableList = client.getTableList(source, queryDTO);
        Assert.assertTrue(CollectionUtils.isNotEmpty(tableList));
    }

    @Test
    public void getDatabaseList() {
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().build();
        List list = client.getAllDatabases(source, queryDTO);
        Assert.assertTrue(CollectionUtils.isNotEmpty(list));
    }

    @Test
    public void getPreview() {
        IClient client = ClientCache.getClient(DataSourceType.MONGODB.getVal());
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
    public void executorQuery() {
        IClient<List> client = ClientCache.getClient(DataSourceType.MONGODB.getVal());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("db.user.find({});").startRow(1).limit(5).build();
        List<Map<String, Object>> result = client.executeQuery(source, queryDTO);
        result.forEach(map->{
            map.keySet().forEach(x->{
                System.out.println(x+"==="+map.get(x));
            });
        });
    }

    /**
     * aggregate
     */
    @Test
    public void executoraggregate() {
        IClient<List> client = ClientCache.getClient(DataSourceType.MONGODB.getVal());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("db.user.aggregate([{$group : {_id : \"$by_user\", num_tutorial : {$sum : 1}}}])").build();
        List<Map<String, Object>> result = client.executeQuery(source, queryDTO);
        result.forEach(map->{
            map.keySet().forEach(x->{
                System.out.println(x+"==="+map.get(x));
            });
        });
    }

    /**
     * count
     */
    @Test
    public void executorCount() {
        IClient<List> client = ClientCache.getClient(DataSourceType.MONGODB.getVal());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql(" db.user.count()").build();
        List<Map<String, Object>> result = client.executeQuery(source, queryDTO);
        result.forEach(map->{
            map.keySet().forEach(x->{
                System.out.println(x+"==="+map.get(x));
            });
        });
    }

    /**
     * findOne
     */
    @Test
    public void executorFindOne() {
        IClient<List> client = ClientCache.getClient(DataSourceType.MONGODB.getVal());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("db.user.findOne({ })").build();
        List<Map<String, Object>> result = client.executeQuery(source, queryDTO);
        result.forEach(map->{
            map.keySet().forEach(x->{
                System.out.println(x+"==="+map.get(x));
            });
        });
    }


    /**
     * findOne
     */
    @Test
    public void executorDistinct() {
        IClient<List> client = ClientCache.getClient(DataSourceType.MONGODB.getVal());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql(" db.inventory.distinct( \"dept\" )").build();
        List<Map<String, Object>> result = client.executeQuery(source, queryDTO);
        result.forEach(map->{
            map.keySet().forEach(x->{
                System.out.println(x+"==="+map.get(x));
            });
        });
    }


}
