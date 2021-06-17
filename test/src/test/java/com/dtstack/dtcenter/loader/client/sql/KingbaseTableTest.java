package com.dtstack.dtcenter.loader.client.sql;

import com.dtstack.dtcenter.loader.cache.pool.config.PoolConfig;
import com.dtstack.dtcenter.loader.client.BaseTest;
import com.dtstack.dtcenter.loader.client.ClientCache;
import com.dtstack.dtcenter.loader.client.IClient;
import com.dtstack.dtcenter.loader.client.ITable;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.UpsertColumnMetaDTO;
import com.dtstack.dtcenter.loader.dto.source.KingbaseSourceDTO;
import com.dtstack.dtcenter.loader.enums.CommandType;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import org.apache.commons.collections.CollectionUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.util.List;

public class KingbaseTableTest extends BaseTest {

    // 构建client
    private static final ITable client = ClientCache.getTable(DataSourceType.KINGBASE8.getVal());

    // 构建数据源信息
    private static final KingbaseSourceDTO source = KingbaseSourceDTO.builder()
            .url("jdbc:kingbase8://172.16.100.186:54321/test_db")
            .username("test")
            .password("test123")
            .poolConfig(PoolConfig.builder().maximumPoolSize(2).build())
            .build();

    /**
     * 数据准备
     */
    @BeforeClass
    public static void beforeClass() {
        IClient client = ClientCache.getClient(DataSourceType.KINGBASE8.getVal());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("drop table if exists LOADER_TEST").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("create table LOADER_TEST (id int, name varchar(50))").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("comment on column LOADER_TEST.id is 'id';").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("comment on column LOADER_TEST.name is '名字';").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("insert into LOADER_TEST values (1, 'LOADER_TEST')").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
    }


    @Test
    public void upsertTableColumn() {
        UpsertColumnMetaDTO columnMetaDTO = new UpsertColumnMetaDTO();
        columnMetaDTO.setCommandType(CommandType.INSERT);
        columnMetaDTO.setSchema("test_db");
        columnMetaDTO.setTableName("LOADER_TEST");
        columnMetaDTO.setColumnComment("comment");
        columnMetaDTO.setColumnName("age");
        columnMetaDTO.setColumnType("int");
        client.upsertTableColumn(source, columnMetaDTO);
    }
}
