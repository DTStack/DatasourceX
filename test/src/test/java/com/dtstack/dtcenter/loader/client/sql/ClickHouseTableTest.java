package com.dtstack.dtcenter.loader.client.sql;

import com.dtstack.dtcenter.loader.cache.pool.config.PoolConfig;
import com.dtstack.dtcenter.loader.client.BaseTest;
import com.dtstack.dtcenter.loader.client.ClientCache;
import com.dtstack.dtcenter.loader.client.IClient;
import com.dtstack.dtcenter.loader.client.ITable;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.UpsertColumnMetaDTO;
import com.dtstack.dtcenter.loader.dto.source.ClickHouseSourceDTO;
import com.dtstack.dtcenter.loader.enums.CommandType;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * @company: www.dtstack.com
 * @Author ：loader_test
 * @Date ：Created in 15:47 2020/2/28
 * @Description：ClickHouse 测试
 */
public class ClickHouseTableTest extends BaseTest {

    private static final ITable client = ClientCache.getTable(DataSourceType.Clickhouse.getVal());

    private static ClickHouseSourceDTO source = ClickHouseSourceDTO.builder()
            .url("jdbc:clickhouse://172.16.100.186:8123")
            .username("default")
            .password("b6rCe7ZV")
            .schema("default")
            .poolConfig(new PoolConfig())
            .build();

    /**
     * 数据准备
     */
    @BeforeClass
    public static void beforeClass() {
        IClient client = ClientCache.getClient(DataSourceType.Clickhouse.getVal());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("drop table if exists loader_test").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("CREATE TABLE loader_test (id String  COMMENT 'ID编码',price double, date Date  COMMENT '日期') ENGINE = MergeTree(date, (id,date), 8192)").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("insert into loader_test values('1',1.2, toDate('2020-08-22'))").build();
        assert client.executeSqlWithoutResultSet(source, queryDTO);
    }

    @Test
    public void getSourceType() {
        Assert.assertEquals(DataSourceType.Clickhouse.getVal(), source.getSourceType());
    }


    @Test
    public void upsertTableColumn() {
        UpsertColumnMetaDTO columnMetaDTO = new UpsertColumnMetaDTO();
        columnMetaDTO.setCommandType(CommandType.INSERT);
        columnMetaDTO.setSchema("default");
        columnMetaDTO.setTableName("loader_test");
        columnMetaDTO.setColumnComment("comment");
        columnMetaDTO.setColumnName("age");
        columnMetaDTO.setColumnType("String");
        client.upsertTableColumn(source, columnMetaDTO);
    }
}
