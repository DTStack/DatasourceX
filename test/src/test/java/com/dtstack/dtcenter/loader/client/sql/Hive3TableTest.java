package com.dtstack.dtcenter.loader.client.sql;

import com.dtstack.dtcenter.loader.client.BaseTest;
import com.dtstack.dtcenter.loader.client.ClientCache;
import com.dtstack.dtcenter.loader.client.IClient;
import com.dtstack.dtcenter.loader.client.ITable;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.UpsertColumnMetaDTO;
import com.dtstack.dtcenter.loader.dto.source.Hive3SourceDTO;
import com.dtstack.dtcenter.loader.enums.CommandType;
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
public class Hive3TableTest extends BaseTest {

    /**
     * 构造hive客户端
     */
    private static final ITable client = ClientCache.getTable(DataSourceType.HIVE3X.getVal());

    /**
     * 构建数据源信息
     */
    private static final Hive3SourceDTO source = Hive3SourceDTO.builder()
            .url("jdbc:hive2://172.16.101.192:2181,172.16.101.120:2181,172.16.101.244:2181/default;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2")
            .schema("default")
            .defaultFS("hdfs://dtstack")
            .config("{\n" +
                    "    \"dfs.ha.namenodes.dtstack\": \"nn1,nn2\",\n" +
                    "    \"dfs.namenode.rpc-address.dtstack.nn1\": \"172.16.101.120:8020\",\n" +
                    "    \"dfs.namenode.rpc-address.dtstack.nn2\": \"172.16.101.244:8020\",\n" +
                    "    \"dfs.client.failover.proxy.provider.dtstack\": \"org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider\",\n" +
                    "    \"dfs.nameservices\": \"dtstack\"\n" +
                    "}")
            .username("hive")
            .build();
    /**
     * 数据准备
     */
    @BeforeClass
    public static void setUp () {
        IClient client = ClientCache.getClient(DataSourceType.HIVE3X.getVal());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("drop table if exists loader_test_part").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("create table loader_test_part (id int comment 'ID', name string comment '姓名_name') partitioned by (pt1 string,pt2 string, pt3 string)").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("insert into  loader_test_part partition (pt1 = 'a1', pt2 = 'b1', pt3 = 'c1') values(1, 'wangcahun')").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("insert into  loader_test_part partition (pt1 = 'a2', pt2 = 'b2', pt3 = 'c2') values(1, 'wangcahun')").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("insert into  loader_test_part partition (pt1 = 'a3', pt2 = 'b3', pt3 = 'c3') values(1, 'wangcahun')").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("drop table if exists loader_test_2").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("create table loader_test_2 (id int comment 'ID', name string comment '姓名_name')").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("drop table if exists loader_test_3").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("create table loader_test_3 (id int comment 'ID', name string comment '姓名_name')").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("drop view if exists loader_test_5").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("create view loader_test_5 as select * from loader_test_3").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
    }

    /**
     * 获取所有分区
     */
    @Test
    public void showPartitions () {
        List<String> result = client.showPartitions(source, "loader_test_part");
        System.out.println(result);
        Assert.assertTrue(CollectionUtils.isNotEmpty(result));
    }

    /**
     * 删除表
     */
    @Test
    public void dropTable () {
        Boolean check = client.dropTable(source, "loader_test_2");
        Assert.assertTrue(check);
    }

    /**
     * 重命名表
     */
    @Test
    public void renameTable () {
        client.executeSqlWithoutResultSet(source, "drop table if exists loader_test_4");
        Boolean renameCheck = client.renameTable(source, "loader_test_3", "loader_test_4");
        Assert.assertTrue(renameCheck);
    }

    /**
     * 修改表参数
     */
    @Test
    public void alterTableParams () {
        Map<String, String> params = Maps.newHashMap();
        params.put("comment", "test");
        Boolean alterCheck = client.alterTableParams(source, "loader_test_part", params);
        Assert.assertTrue(alterCheck);
    }

    /**
     * 判断表是否是视图 - 是
     */
    @Test
    public void tableIsView () {
        ITable client = ClientCache.getTable(DataSourceType.HIVE.getVal());
        Boolean check = client.isView(source, null, "loader_test_5");
        Assert.assertTrue(check);
    }

    /**
     * 判断表是否是视图 - 否
     */
    @Test
    public void tableIsNotView () {
        ITable client = ClientCache.getTable(DataSourceType.HIVE.getVal());
        Boolean check = client.isView(source, null, "loader_test_5");
        Assert.assertTrue(check);
    }

    @Test
    public void upsertTableColumn() {
        UpsertColumnMetaDTO columnMetaDTO = new UpsertColumnMetaDTO();
        columnMetaDTO.setCommandType(CommandType.INSERT);
        columnMetaDTO.setSchema("default");
        columnMetaDTO.setTableName("loader_test_part");
        columnMetaDTO.setColumnComment("comment");
        columnMetaDTO.setColumnName("age");
        columnMetaDTO.setColumnType("int");
        client.upsertTableColumn(source, columnMetaDTO);
    }
}
