package com.dtstack.dtcenter.loader.client.sql;

import com.dtstack.dtcenter.loader.cache.pool.config.PoolConfig;
import com.dtstack.dtcenter.loader.client.ClientCache;
import com.dtstack.dtcenter.loader.client.IClient;
import com.dtstack.dtcenter.loader.client.ITable;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.source.ImpalaSourceDTO;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import com.google.common.collect.Maps;
import org.apache.commons.collections.CollectionUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.util.List;
import java.util.Map;

/**
 * spark table测试
 *
 * @author ：wangchuan
 * date：Created in 10:14 上午 2020/12/7
 * company: www.dtstack.com
 */
public class ImpalaTableTest {

    private static ImpalaSourceDTO source = ImpalaSourceDTO.builder()
            .url("jdbc:impala://172.16.100.226:21050/shier;AuthMech=3")
            .username("hxb")
            .password("admin123")
            .poolConfig(new PoolConfig())
            .build();

    /**
     * 数据准备
     */
    @BeforeClass
    public static void setUp () {
        IClient client = ClientCache.getClient(DataSourceType.IMPALA.getVal());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("drop table if exists wangchuan_partitions_test").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("create table wangchuan_partitions_test (id int, name string) partitioned by (pt1 string,pt2 string, pt3 string)").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("insert into  wangchuan_partitions_test partition (pt1 = 'a1', pt2 = 'b1', pt3 = 'c1') values(1, 'wangcahun')").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("insert into  wangchuan_partitions_test partition (pt1 = 'a2', pt2 = 'b2', pt3 = 'c2') values(1, 'wangcahun')").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("insert into  wangchuan_partitions_test partition (pt1 = 'a3', pt2 = 'b3', pt3 = 'c3') values(1, 'wangcahun')").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("drop table if exists wangchuan_test2").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("create table wangchuan_test2 (id int, name string)").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("drop table if exists wangchuan_test3").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("create table wangchuan_test3 (id int, name string)").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
    }

    /**
     * 获取所有分区
     */
    @Test
    public void showPartitions () {
        ITable client = ClientCache.getTable(DataSourceType.IMPALA.getVal());
        List<String> result = client.showPartitions(source, "wangchuan_partitions_test");
        System.out.println(result);
        Assert.assertTrue(CollectionUtils.isNotEmpty(result));
    }

    /**
     * 删除表
     */
    @Test
    public void dropTable () {
        ITable client = ClientCache.getTable(DataSourceType.IMPALA.getVal());
        Boolean check = client.dropTable(source, "wangchuan_test2");
        Assert.assertTrue(check);
    }

    /**
     * 重命名表
     */
    @Test
    public void renameTable () {
        ITable client = ClientCache.getTable(DataSourceType.IMPALA.getVal());
        Boolean renameCheck = client.renameTable(source, "wangchuan_test3", "wangchuan_test4");
        Assert.assertTrue(renameCheck);
        Boolean dropCheck = client.dropTable(source, "wangchuan_test4");
        Assert.assertTrue(dropCheck);
    }

    /**
     * 修改表参数
     */
    @Test
    public void alterTableParams () {
        ITable client = ClientCache.getTable(DataSourceType.IMPALA.getVal());
        Map<String, String> params = Maps.newHashMap();
        params.put("comment", "test");
        Boolean alterCheck = client.alterTableParams(source, "wangchuan_partitions_test", params);
        Assert.assertTrue(alterCheck);
    }
}
