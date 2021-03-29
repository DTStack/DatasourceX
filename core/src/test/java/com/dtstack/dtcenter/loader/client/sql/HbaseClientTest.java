package com.dtstack.dtcenter.loader.client.sql;

import com.dtstack.dtcenter.loader.cache.pool.config.PoolConfig;
import com.dtstack.dtcenter.loader.client.ClientCache;
import com.dtstack.dtcenter.loader.client.IHbase;
import com.dtstack.dtcenter.loader.dto.source.HbaseSourceDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.collections.CollectionUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * hbase新客户端无kerberos认证测试
 *
 * @author ：wangchuan
 * date：Created in 9:38 上午 2020/12/2
 * company: www.dtstack.com
 */
public class HbaseClientTest {

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
            HBASE_CLIENT.createHbaseTable(source, "loader_test_2", new String[]{"info1", "info2"});
        } catch (Exception e) {
            // 目前插件化里没有方法支持判断表是否存在，异常不作处理
        }
        HBASE_CLIENT.putRow(source, "loader_test_2", "1001", "info1", "name", "wangchuan");
        HBASE_CLIENT.putRow(source, "loader_test_2", "1002", "info1", "name", "wangbin");
        HBASE_CLIENT.putRow(source, "loader_test_2", "1003", "info2", "name", "wangchuan");
        HBASE_CLIENT.putRow(source, "loader_test_2", "1003", "info2", "age", "18");
        HBASE_CLIENT.putRow(source, "loader_test_2", "1004_loader", "info2", "addr", "beijing");
        HBASE_CLIENT.putRow(source, "loader_test_2", "1005_loader", "info2", "addr", "shanghai");
        HBASE_CLIENT.putRow(source, "loader_test_2", "1006_loader", "info2", "addr", "shenzhen");
        HBASE_CLIENT.putRow(source, "loader_test_2", "1007_loader", "info2", "addr", "hangzhou");
    }

    /**
     * 测试已经存在的namespace
     */
    @Test
    public void dbExists() {
        IHbase hbaseClient = ClientCache.getHbase(DataSourceType.HBASE.getVal());
        Boolean check = hbaseClient.isDbExists(source, "default");
        Assert.assertTrue(check);
    }

    /**
     * 测试不存在的namespace
     */
    @Test
    public void dbNotExists() {
        Boolean check = HBASE_CLIENT.isDbExists(source, UUID.randomUUID().toString());
        Assert.assertFalse(check);
    }

    /**
     * 创建已经存在的表测试
     */
    @Test(expected = DtLoaderException.class)
    public void createHbaseTableExists() {
        Boolean check = HBASE_CLIENT.createHbaseTable(source, "loader_test_2", new String[]{"info1", "info2"});
        Assert.assertFalse(check);
    }

    /**
     * 已注try-catch
     * 创建已经存在的表测试，需要测试自己手动修改表名，目前暂时不支持hbase删除表
     */
    @Test
    public void createHbaseTableNotExists() {
        try {
            Boolean check = HBASE_CLIENT.createHbaseTable(source, "_tableName", new String[]{"info1", "info2"});
            System.out.println(check);
        } catch (Exception e){
            // 不作处理
        }
    }

    /**
     * 根据rowKey正则获取对应的rowKey列表
     */
    @Test
    public void scanByRegex() {
        List<String> list = HBASE_CLIENT.scanByRegex(source, "loader_test_2", ".*_loader");
        Assert.assertTrue(CollectionUtils.isNotEmpty(list));
    }

    /**
     * 插入指定rowKey、列族、列名的数据
     */
    @Test
    public void putRow() {
        Boolean check = HBASE_CLIENT.putRow(source, "loader_test_2", "1002", "info1", "name", "wangchuan");
        Assert.assertTrue(check);
    }

    /**
     * 获取指定rowKey、列族、列名的数据
     */
    @Test
    public void getRow(){
        String row = HBASE_CLIENT.getRow(source, "loader_test_2", "1003", "info2", "name");
        Assert.assertTrue(org.apache.commons.lang3.StringUtils.isNotBlank(row));
    }

    /**
     * 删除指定rowKey、列族、列名的数据
     */
    @Test
    public void deleteByRowKey() {
        Boolean check = HBASE_CLIENT.deleteByRowKey(source, "loader_test_2", "info1", "name", Lists.newArrayList("1001", "1002"));
        Assert.assertTrue(check);
    }

    /**
     * 数据预览
     */
    @Test
    public void preview() {
        List<List<String>> preview = HBASE_CLIENT.preview(source, "loader_test_2", 10);
        Assert.assertTrue(CollectionUtils.isNotEmpty(preview));
    }

    /**
     * 数据预览
     */
    @Test
    public void preview2() {
        List<List<String>> preview = HBASE_CLIENT.preview(source, "loader_test_2", Lists.newArrayList("info2"), 1);
        Assert.assertTrue(CollectionUtils.isNotEmpty(preview));
    }

    /**
     * 数据预览
     */
    @Test
    public void preview3() {
        Map<String, List<String>> familyQualifierMap = Maps.newHashMap();
        List<String> info1Columns = Lists.newArrayList("name", "age");
        List<String> info2Columns = Lists.newArrayList("addr", "name", "age");
        familyQualifierMap.put("info1", info1Columns);
        familyQualifierMap.put("info2", info2Columns);
        List<List<String>> preview = HBASE_CLIENT.preview(source, "loader_test_2", familyQualifierMap, 10);
        Assert.assertTrue(CollectionUtils.isNotEmpty(preview));
    }

    /**
     * 数据预览 无数据测试
     */
    @Test
    public void preview4() {
        Map<String, List<String>> familyQualifierMap = Maps.newHashMap();
        List<String> info1Columns = Lists.newArrayList("xxx");
        familyQualifierMap.put("info1", info1Columns);
        List<List<String>> preview = HBASE_CLIENT.preview(source, "loader_test_2", familyQualifierMap, 10);
        Assert.assertEquals(0, preview.size());
    }
}
