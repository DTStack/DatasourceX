package com.dtstack.dtcenter.loader.client.sql;

import com.dtstack.dtcenter.loader.cache.pool.config.PoolConfig;
import com.dtstack.dtcenter.loader.client.BaseTest;
import com.dtstack.dtcenter.loader.client.ClientCache;
import com.dtstack.dtcenter.loader.client.IClient;
import com.dtstack.dtcenter.loader.client.ITable;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.UpsertColumnMetaDTO;
import com.dtstack.dtcenter.loader.dto.source.Db2SourceDTO;
import com.dtstack.dtcenter.loader.enums.CommandType;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 22:30 2020/2/28
 * @Description：DB2 测试
 */
@Slf4j
public class Db2TableTest extends BaseTest {

    // 获取数据源 client
    private static final ITable client = ClientCache.getTable(DataSourceType.DB2.getVal());

    private static final Db2SourceDTO source = Db2SourceDTO.builder()
            .url("jdbc:db2://172.16.101.246:50002/DT_TEST")
            .username("db2inst1")
            .password("dtstack1")
            .poolConfig(new PoolConfig())
            .build();

    /**
     * 数据准备
     */
    @BeforeClass
    public static void beforeClass() {
        IClient client = ClientCache.getClient(DataSourceType.DB2.getVal());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("drop table LOADER_TEST").build();
        try {
            client.executeSqlWithoutResultSet(source, queryDTO);
        } catch (Exception e) {
            // db2 不支持drop table if exists语法
        }
        queryDTO = SqlQueryDTO.builder().sql("create table LOADER_TEST (id int, name varchar(50))").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("COMMENT ON COLUMN LOADER_TEST.id IS 'id'").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("COMMENT ON COLUMN LOADER_TEST.name IS '姓名'").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("comment on table LOADER_TEST is '中文_table_comment'").build();
        client.executeSqlWithoutResultSet(source, queryDTO);

        queryDTO = SqlQueryDTO.builder().sql("insert into LOADER_TEST values (1, 'nanqi')").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
    }


    /**
     * 获取连接测试
     */
    @Test
    public void getCon() throws Exception{
        Connection connection = client.getCon(source);
        Assert.assertNotNull(connection);
        connection.close();
    }

    @Test
    public void upsertTableColumn() {
        UpsertColumnMetaDTO columnMetaDTO = new UpsertColumnMetaDTO();
        columnMetaDTO.setCommandType(CommandType.INSERT);
        columnMetaDTO.setSchema("DT_TEST");
        columnMetaDTO.setTableName("LOADER_TEST");
        columnMetaDTO.setColumnComment("comment");
        columnMetaDTO.setColumnName("age");
        columnMetaDTO.setColumnType("int");
        client.upsertTableColumn(source, columnMetaDTO);
    }

}
