package com.dtstack.dtcenter.loader.client.sql;

import com.dtstack.dtcenter.loader.client.ClientCache;
import com.dtstack.dtcenter.loader.client.IClient;
import com.dtstack.dtcenter.loader.client.ITable;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.source.LibraSourceDTO;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Map;

/**
 * Libra table测试
 *
 * @author ：wangchuan
 * date：Created in 10:14 上午 2020/12/7
 * company: www.dtstack.com
 */
public class LibraTableTest {

    private static LibraSourceDTO source = LibraSourceDTO.builder()
            .url("jdbc:postgresql://kudu5:54321/database?currentSchema=public")
            .username("postgres")
            .password("password")
            .build();

    /**
     * 数据准备
     */
    @BeforeClass
    public static void setUp () throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.LIBRA.getVal());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("drop table if exists wangchuan_test1").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("create table wangchuan_test1 (id int)").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("drop table if exists wangchuan_test2").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("create table wangchuan_test2 (id int)").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("create table wangchuan_test5 (id int)").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("insert into wangchuan_test5 values (1),(2),(1),(2),(1),(2),(1),(2),(1),(2),(1),(2)").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
    }

    /**
     * 重命名表
     */
    @Test
    public void dropTable () throws Exception {
        ITable client = ClientCache.getTable(DataSourceType.LIBRA.getVal());
        Boolean dropCheck = client.dropTable(source, "wangchuan_test1");
        Assert.assertTrue(dropCheck);
    }

    /**
     * 重命名表
     */
    @Test
    public void renameTable () throws Exception {
        ITable client = ClientCache.getTable(DataSourceType.LIBRA.getVal());
        Boolean renameCheck1 = client.renameTable(source, "wangchuan_test2", "wangchuan_test3");
        Assert.assertTrue(renameCheck1);
        Boolean renameCheck2 = client.renameTable(source, "wangchuan_test3", "wangchuan_test2");
        Assert.assertTrue(renameCheck2);
    }

    /**
     * 重命名表
     */
    @Test
    public void alterParamTable () throws Exception {
        ITable client = ClientCache.getTable(DataSourceType.LIBRA.getVal());
        Map<String, String> params = Maps.newHashMap();
        params.put("comment", "test");
        Boolean alterParamCheck = client.alterTableParams(source, "wangchuan_test2", params);
        Assert.assertTrue(alterParamCheck);
    }

    /**
     * 重命名表
     */
    @Test
    public void getTableSize () throws Exception {
        ITable tableClient = ClientCache.getTable(DataSourceType.LIBRA.getVal());
        Long tableSize = tableClient.getTableSize(source, "public", "wangchuan_test5");
        System.out.println(tableSize);
        Assert.assertTrue(tableSize != null && tableSize > 0);
    }
}
