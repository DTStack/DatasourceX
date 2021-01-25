package com.dtstack.dtcenter.loader.client.sql;

import com.dtstack.dtcenter.loader.client.ClientCache;
import com.dtstack.dtcenter.loader.client.IClient;
import com.dtstack.dtcenter.loader.client.ITable;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.source.Hive1SourceDTO;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import com.google.common.collect.Maps;
import org.apache.commons.collections.CollectionUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;
import java.util.Map;

/**
 * hive1x table测试
 *
 * @author ：wangchuan
 * date：Created in 10:14 上午 2020/12/7
 * company: www.dtstack.com
 */
public class Hive1xTableTest {

    /**
     * 构造hive客户端
     */
    private static final ITable client = ClientCache.getTable(DataSourceType.HIVE1X.getVal());

    /**
     * 构建数据源信息
     */
    private static final Hive1SourceDTO source = Hive1SourceDTO.builder()
            .url("jdbc:hive2://172.16.10.67:10000/default")
            .username("root")
            .password("abc123")
            .defaultFS("hdfs://172.16.10.67:8020")
            .build();

    /**
     * 数据准备
     */
    @BeforeClass
    public static void setUp () {
        IClient client = ClientCache.getClient(DataSourceType.HIVE1X.getVal());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("drop table if exists loader_test_part").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("create table loader_test_part (id int, name string) partitioned by (pt1 string,pt2 string, pt3 string)").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("insert into  loader_test_part partition (pt1 = 'a1', pt2 = 'b1', pt3 = 'c1') values(1, 'wangcahun')").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("insert into  loader_test_part partition (pt1 = 'a2', pt2 = 'b2', pt3 = 'c2') values(1, 'wangcahun')").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("insert into  loader_test_part partition (pt1 = 'a3', pt2 = 'b3', pt3 = 'c3') values(1, 'wangcahun')").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("drop table if exists loader_test_2").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("create table loader_test_2 (id int, name string)").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("drop table if exists loader_test_3").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("create table loader_test_3 (id int, name string)").build();
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
        ITable client = ClientCache.getTable(DataSourceType.HIVE1X.getVal());
        Boolean check = client.isView(source, null, "wangchuan_test5");
        Assert.assertTrue(check);
    }

    /**
     * 判断表是否是视图 - 否
     */
    @Test
    public void tableIsNotView () {
        ITable client = ClientCache.getTable(DataSourceType.HIVE1X.getVal());
        Boolean check = client.isView(source, null, "wangchuan_test3");
        Assert.assertFalse(check);
    }
}
