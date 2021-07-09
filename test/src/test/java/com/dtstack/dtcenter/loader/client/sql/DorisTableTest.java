package com.dtstack.dtcenter.loader.client.sql;

import com.dtstack.dtcenter.loader.cache.pool.config.PoolConfig;
import com.dtstack.dtcenter.loader.client.BaseTest;
import com.dtstack.dtcenter.loader.client.ClientCache;
import com.dtstack.dtcenter.loader.client.IClient;
import com.dtstack.dtcenter.loader.client.ITable;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.UpsertColumnMetaDTO;
import com.dtstack.dtcenter.loader.dto.source.DorisSourceDTO;
import com.dtstack.dtcenter.loader.enums.CommandType;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class DorisTableTest extends BaseTest {

    /**
     * 构造doris客户端
     */
    private static final ITable client = ClientCache.getTable(DataSourceType.DORIS.getVal());

    // 构建数据源信息
    private static final DorisSourceDTO source = DorisSourceDTO.builder()
            .url("jdbc:mysql://172.16.21.49:9030/qianyi")
            .username("root")
            .poolConfig(PoolConfig.builder().build())
            .build();


    /**
     * 数据预处理
     */
    @BeforeClass
    public static void beforeClass() {
        // 获取数据源 client
        IClient client = ClientCache.getClient(DataSourceType.DORIS.getVal());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("drop table if exists LOADER_TEST").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("create table LOADER_TEST (id int COMMENT 'id', name varchar(50) COMMENT '姓名') COMMENT \"table comment\" DISTRIBUTED BY HASH(id) BUCKETS 10;").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("insert into LOADER_TEST values (1, 'LOADER_TEST')").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("drop view if exists LOADER_TEST_VIEW").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("create view LOADER_TEST_VIEW as select * from LOADER_TEST").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
    }


    @Test(expected = DtLoaderException.class)
    public void showPartitions() {
        client.showPartitions(source, "qianyi");
    }


    @Test(expected = DtLoaderException.class)
    public void alterTableParams() {
        Map<String, String> param = new HashMap<>();
        param.put("comment", "aaa");
        client.alterTableParams(source, "LOADER_TEST", param);
    }


    @Test
    public void upsertTableColumn() {
        UpsertColumnMetaDTO columnMetaDTO = new UpsertColumnMetaDTO();
        columnMetaDTO.setCommandType(CommandType.INSERT);
        columnMetaDTO.setSchema("qianyi");
        columnMetaDTO.setTableName("LOADER_TEST");
        columnMetaDTO.setColumnComment("comment");
        columnMetaDTO.setColumnName("age");
        columnMetaDTO.setColumnType("int");
        client.upsertTableColumn(source, columnMetaDTO);
    }

}
