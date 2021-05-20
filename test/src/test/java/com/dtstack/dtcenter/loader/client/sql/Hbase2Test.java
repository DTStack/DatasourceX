package com.dtstack.dtcenter.loader.client.sql;

import com.dtstack.dtcenter.loader.cache.pool.config.PoolConfig;
import com.dtstack.dtcenter.loader.client.BaseTest;
import com.dtstack.dtcenter.loader.client.ClientCache;
import com.dtstack.dtcenter.loader.client.IClient;
import com.dtstack.dtcenter.loader.client.IHbase;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.source.HbaseSourceDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import org.apache.commons.collections.CollectionUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;
import java.util.UUID;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 17:43 2020/7/9
 * @Description：Hbase 2 测试
 */
public class Hbase2Test extends BaseTest {

    // 构建client
    private static final IClient client = ClientCache.getClient("hbase2");

    // 构建hbase client
    private static final IHbase HBASE_CLIENT = ClientCache.getHbase(DataSourceType.HBASE.getVal());

    // 连接池信息
    private static final PoolConfig poolConfig = PoolConfig.builder().maximumPoolSize(100).build();

    // 构建数据源信息
    private static final HbaseSourceDTO source = HbaseSourceDTO.builder()
            .url("172.16.100.175:2181,172.16.101.196:2181,172.16.101.227:2181")
            .path("/hbase2")
            .poolConfig(poolConfig)
            .build();

    /**
     * 数据准备
     */
    @BeforeClass
    public static void setUp () {
        try {
            HBASE_CLIENT.createHbaseTable(source, "loader_test", new String[]{"info"});
        } catch (Exception e) {
            // 目前没有支持没有删除表的方法
        }
        HBASE_CLIENT.putRow(source, "loader_test", UUID.randomUUID().toString(), "info", "uuid", UUID.randomUUID().toString());
        HBASE_CLIENT.putRow(source, "loader_test", UUID.randomUUID().toString(), "info", "uuid", UUID.randomUUID().toString());
        HBASE_CLIENT.putRow(source, "loader_test", UUID.randomUUID().toString(), "info", "uuid", UUID.randomUUID().toString());
    }

    @Test
    public void testCon() throws Exception {
        Boolean isConnected = client.testCon(source);
        if (Boolean.FALSE.equals(isConnected)) {
            throw new DtLoaderException("connection exception");
        }
    }

    @Test
    public void getTableList() {
        List<String> tableList = client.getTableList(source, null);
        Assert.assertTrue(CollectionUtils.isNotEmpty(tableList));
    }

    @Test
    public void getColumnMetaData() {
        List metaData = client.getColumnMetaData(source, SqlQueryDTO.builder().tableName("loader_test").build());
        Assert.assertTrue(CollectionUtils.isNotEmpty(metaData));
    }

    @Test
    public void executorQuery() {
        SqlQueryDTO sqlQueryDTO = SqlQueryDTO.builder().tableName("loader_test").build();
        List result = client.executeQuery(source, sqlQueryDTO);
        Assert.assertTrue(CollectionUtils.isNotEmpty(result));
    }

    @Test
    public void preview() {
        List<List<Object>> result = client.getPreview(source, SqlQueryDTO.builder().tableName("loader_test").previewNum(2).build());
        Assert.assertTrue(CollectionUtils.isNotEmpty(result));
    }
}
