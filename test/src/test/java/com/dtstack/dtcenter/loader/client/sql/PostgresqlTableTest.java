package com.dtstack.dtcenter.loader.client.sql;

import com.dtstack.dtcenter.loader.cache.pool.config.PoolConfig;
import com.dtstack.dtcenter.loader.client.BaseTest;
import com.dtstack.dtcenter.loader.client.ClientCache;
import com.dtstack.dtcenter.loader.client.IClient;
import com.dtstack.dtcenter.loader.client.ITable;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.UpsertColumnMetaDTO;
import com.dtstack.dtcenter.loader.dto.source.PostgresqlSourceDTO;
import com.dtstack.dtcenter.loader.enums.CommandType;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * postgresql table测试
 *
 * @author ：wangchuan
 * date：Created in 10:14 上午 2020/12/7
 * company: www.dtstack.com
 */
public class PostgresqlTableTest extends BaseTest {

    private static PostgresqlSourceDTO source = PostgresqlSourceDTO.builder()
            .url("jdbc:postgresql://172.16.101.246:5432/postgres")
            .username("postgres")
            .password("abc123")
            .schema("public")
            .poolConfig(new PoolConfig())
            .build();

    /**
     * 数据准备
     */
    @BeforeClass
    public static void setUp() {
        IClient client = ClientCache.getClient(DataSourceType.PostgreSQL.getVal());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("drop view if exists pg_test_view").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("drop table if exists pg_test1").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("create table pg_test1 (id int)").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("drop table if exists pg_test2").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("create table pg_test2 (id int)").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("drop table if exists pg_test4").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("create table pg_test4 (id int)").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("insert into pg_test4 values (1),(2),(1),(2),(1),(2),(1),(2),(1),(2),(1),(2)").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("create view pg_test_view as select * from pg_test4").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
    }

    /**
     * 重命名表
     */
    @Test
    public void dropTable () {
        ITable client = ClientCache.getTable(DataSourceType.PostgreSQL.getVal());
        Boolean dropCheck = client.dropTable(source, "pg_test1");
        Assert.assertTrue(dropCheck);
    }

    /**
     * 重命名表
     */
    @Test
    public void renameTable () {
        ITable client = ClientCache.getTable(DataSourceType.PostgreSQL.getVal());
        Boolean renameCheck1 = client.renameTable(source, "pg_test2", "pg_test3");
        Assert.assertTrue(renameCheck1);
        Boolean renameCheck2 = client.renameTable(source, "pg_test3", "pg_test2");
        Assert.assertTrue(renameCheck2);
    }

    /**
     * 重命名表
     */
    @Test
    public void getTableSize () {
        ITable tableClient = ClientCache.getTable(DataSourceType.PostgreSQL.getVal());
        Long tableSize = tableClient.getTableSize(source, "public", "pg_test4");
        System.out.println(tableSize);
        Assert.assertTrue(tableSize != null && tableSize > 0);
    }

    /**
     * 判断表是否是视图 - 是
     */
    @Test
    public void tableIsView () {
        ITable client = ClientCache.getTable(DataSourceType.PostgreSQL.getVal());
        Boolean check = client.isView(source, null, "pg_test_view");
        Assert.assertTrue(check);
    }

    /**
     * 判断表是否是视图 - 否
     */
    @Test
    public void tableIsNotView () {
        ITable client = ClientCache.getTable(DataSourceType.PostgreSQL.getVal());
        Boolean check = client.isView(source, null, "pg_test4");
        Assert.assertFalse(check);
    }

    @Test
    public void upsertTableColumn() {
        ITable client = ClientCache.getTable(DataSourceType.PostgreSQL.getVal());
        UpsertColumnMetaDTO columnMetaDTO = new UpsertColumnMetaDTO();
        columnMetaDTO.setCommandType(CommandType.INSERT);
        columnMetaDTO.setSchema("public");
        columnMetaDTO.setTableName("pg_test4");
        columnMetaDTO.setColumnComment("comment");
        columnMetaDTO.setColumnName("age");
        columnMetaDTO.setColumnType("int");
        client.upsertTableColumn(source, columnMetaDTO);
    }
}
