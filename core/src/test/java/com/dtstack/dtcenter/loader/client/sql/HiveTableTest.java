package com.dtstack.dtcenter.loader.client.sql;

import com.dtstack.dtcenter.loader.client.ClientCache;
import com.dtstack.dtcenter.loader.client.IClient;
import com.dtstack.dtcenter.loader.client.ITable;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.source.HiveSourceDTO;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import com.google.common.collect.Maps;
import org.apache.commons.collections.CollectionUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;
import java.util.Map;

/**
 * hive table测试
 *
 * @author ：wangchuan
 * date：Created in 10:14 上午 2020/12/7
 * company: www.dtstack.com
 */
public class HiveTableTest {

    private static HiveSourceDTO source = HiveSourceDTO.builder()
            .url("jdbc:hive2://kudu1:10000/dev")
            .schema("dev")
            .defaultFS("hdfs://ns1")
            .username("admin")
            .config("{\n" +
                    "    \"dfs.ha.namenodes.ns1\": \"nn1,nn2\",\n" +
                    "    \"dfs.namenode.rpc-address.ns1.nn2\": \"kudu2:9000\",\n" +
                    "    \"dfs.client.failover.proxy.provider.ns1\": \"org.apache.hadoop.hdfs.server.namenode.ha" +
                    ".ConfiguredFailoverProxyProvider\",\n" +
                    "    \"dfs.namenode.rpc-address.ns1.nn1\": \"kudu1:9000\",\n" +
                    "    \"dfs.nameservices\": \"ns1\"\n" +
                    "}")
            .build();

    /**
     * 数据准备
     */
    @BeforeClass
    public static void setUp () throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.HIVE.getVal());
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
        queryDTO = SqlQueryDTO.builder().sql("drop view if exists wangchuan_test5").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("create view wangchuan_test5 as select * from wangchuan_test3").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
    }

    /**
     * 获取所有分区
     */
    @Test
    public void showPartitions () throws Exception {
        ITable client = ClientCache.getTable(DataSourceType.HIVE.getVal());
        List<String> result = client.showPartitions(source, "wangchuan_partitions_test");
        System.out.println(result);
        Assert.assertTrue(CollectionUtils.isNotEmpty(result));
    }

    /**
     * 删除表
     */
    @Test
    public void dropTable () throws Exception {
        ITable client = ClientCache.getTable(DataSourceType.HIVE.getVal());
        Boolean check = client.dropTable(source, "wangchuan_test2");
        Assert.assertTrue(check);
    }

    /**
     * 重命名表
     */
    @Test
    public void renameTable () throws Exception {
        ITable client = ClientCache.getTable(DataSourceType.HIVE.getVal());
        Boolean renameCheck = client.renameTable(source, "wangchuan_test3", "wangchuan_test4");
        Assert.assertTrue(renameCheck);
        Boolean dropCheck = client.dropTable(source, "wangchuan_test4");
        Assert.assertTrue(dropCheck);
    }

    /**
     * 修改表参数
     */
    @Test
    public void alterTableParams () throws Exception {
        ITable client = ClientCache.getTable(DataSourceType.HIVE.getVal());
        Map<String, String> params = Maps.newHashMap();
        params.put("comment", "test");
        Boolean alterCheck = client.alterTableParams(source, "wangchuan_partitions_test", params);
        Assert.assertTrue(alterCheck);
    }

    /**
     * 判断表是否是视图 - 是
     */
    @Test
    public void tableIsView () {
        ITable client = ClientCache.getTable(DataSourceType.HIVE.getVal());
        Boolean check = client.isView(source, null, "wangchuan_test5");
        Assert.assertTrue(check);
    }

    /**
     * 判断表是否是视图 - 否
     */
    @Test
    public void tableIsNotView () {
        ITable client = ClientCache.getTable(DataSourceType.HIVE.getVal());
        Boolean check = client.isView(source, null, "wangchuan_test3");
        Assert.assertFalse(check);
    }
}
