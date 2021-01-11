package com.dtstack.dtcenter.loader.client.sql;

import com.dtstack.dtcenter.loader.client.ClientCache;
import com.dtstack.dtcenter.loader.client.IClient;
import com.dtstack.dtcenter.loader.client.ITable;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.source.Greenplum6SourceDTO;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * greenplum table测试
 *
 * @author ：wangchuan
 * date：Created in 10:14 上午 2020/12/7
 * company: www.dtstack.com
 */
public class GreenplumTableTest {

    private static Greenplum6SourceDTO source = Greenplum6SourceDTO.builder()
            .url("jdbc:pivotal:greenplum://172.16.10.90:5432;DatabaseName=data")
            .username("gpadmin")
            .password("gpadmin")
            .schema("public")
            .build();

    @BeforeClass
    public static void setUp () {
        IClient client = ClientCache.getClient(DataSourceType.GREENPLUM6.getVal());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("drop view if exists gp_test_view").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("drop table if exists gp_test").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("create table gp_test (id integer, name text)").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("insert into gp_test values (1, 'gpp')").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("create view gp_test_view as select * from gp_test").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
    }

    /**
     * 重命名表
     */
    @Test
    public void renameTable () throws Exception {
        ITable client = ClientCache.getTable(DataSourceType.GREENPLUM6.getVal());
        Boolean renameCheck1 = client.renameTable(source, "wangchuan_test2", "wangchuan_test3");
        Assert.assertTrue(renameCheck1);
        Boolean renameCheck2 = client.renameTable(source, "wangchuan_test3", "wangchuan_test2");
        Assert.assertTrue(renameCheck2);
    }

    /**
     * 获取表大小
     */
    @Test
    public void getTableSize () throws Exception {
        ITable tableClient = ClientCache.getTable(DataSourceType.GREENPLUM6.getVal());
        Long tableSize = tableClient.getTableSize(source, "public", "wangchuan_123");
        System.out.println(tableSize);
        Assert.assertTrue(tableSize != null && tableSize > 0);
    }

    /**
     * 判断表是否是视图 - 是
     */
    @Test
    public void tableIsView () {
        ITable client = ClientCache.getTable(DataSourceType.GREENPLUM6.getVal());
        Boolean check = client.isView(source, null, "gp_test_view");
        Assert.assertTrue(check);
    }

    /**
     * 判断表是否是视图 - 否
     */
    @Test
    public void tableIsNotView () {
        ITable client = ClientCache.getTable(DataSourceType.GREENPLUM6.getVal());
        Boolean check = client.isView(source, null, "gp_test");
        Assert.assertFalse(check);
    }
}
