package com.dtstack.dtcenter.loader.client.sql;

import com.dtstack.dtcenter.loader.cache.pool.config.PoolConfig;
import com.dtstack.dtcenter.loader.client.ClientCache;
import com.dtstack.dtcenter.loader.client.ITable;
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

    // 获取数据源 client
    private static final ITable client = ClientCache.getTable(DataSourceType.GREENPLUM6.getVal());

    private static final Greenplum6SourceDTO source = Greenplum6SourceDTO.builder()
            .url("jdbc:pivotal:greenplum://172.16.100.186:5432;DatabaseName=postgres")
            .username("gpadmin")
            .password("gpadmin")
            .schema("public")
            .poolConfig(new PoolConfig())
            .build();

    /**
     * 数据准备
     */
    @BeforeClass
    public static void beforeClass() {
        client.executeSqlWithoutResultSet(source, "drop table if exists loader_test");
        client.executeSqlWithoutResultSet(source, "create table loader_test (id int, name text)");
        client.executeSqlWithoutResultSet(source, "comment on table loader_test is 'table comment'");
        client.executeSqlWithoutResultSet(source, "insert into loader_test values (1, 'nanqi')");
        client.executeSqlWithoutResultSet(source, "drop table if exists loader_test_2");
        client.executeSqlWithoutResultSet(source, "create table loader_test_2 (id int, name text)");
    }

    /**
     * 重命名表
     */
    @Test
    public void renameTable () {
        ITable client = ClientCache.getTable(DataSourceType.GREENPLUM6.getVal());
        Boolean renameCheck1 = client.renameTable(source, "loader_test_2", "loader_test_3");
        Assert.assertTrue(renameCheck1);
        Boolean renameCheck2 = client.renameTable(source, "loader_test_3", "loader_test_2");
        Assert.assertTrue(renameCheck2);
    }

    /**
     * 获取表大小
     */
    @Test
    public void getTableSize () {
        ITable tableClient = ClientCache.getTable(DataSourceType.GREENPLUM6.getVal());
        Long tableSize = tableClient.getTableSize(source, "public", "loader_test");
        System.out.println(tableSize);
        Assert.assertTrue(tableSize != null && tableSize > 0);
    }

}
