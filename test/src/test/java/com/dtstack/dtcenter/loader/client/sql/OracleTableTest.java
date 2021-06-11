package com.dtstack.dtcenter.loader.client.sql;

import com.dtstack.dtcenter.loader.cache.pool.config.PoolConfig;
import com.dtstack.dtcenter.loader.client.BaseTest;
import com.dtstack.dtcenter.loader.client.ClientCache;
import com.dtstack.dtcenter.loader.client.IClient;
import com.dtstack.dtcenter.loader.client.ITable;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.UpsertColumnMetaDTO;
import com.dtstack.dtcenter.loader.dto.source.OracleSourceDTO;
import com.dtstack.dtcenter.loader.enums.CommandType;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;

public class OracleTableTest extends BaseTest {

    // 构造客户端
    private static final ITable client = ClientCache.getTable(DataSourceType.Oracle.getVal());

    // 数据源信息
    private static final OracleSourceDTO source = OracleSourceDTO.builder()
            .url("jdbc:oracle:thin:@172.16.100.243:1521:orcl")
            .username("oracle")
            .password("oracle")
            .schema("ORACLE")
            .poolConfig(new PoolConfig())
            .build();

    /**
     * 单元测试需要的数据准备
     */
    @BeforeClass
    public static void beforeClass()  {
        IClient client = ClientCache.getClient(DataSourceType.Oracle.getVal());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("drop table LOADER_TEST").build();
        try {
            client.executeSqlWithoutResultSet(source, queryDTO);
        } catch (Exception e){
            // oracle不支持 drop table if exists tableName 语法
        }
        queryDTO = SqlQueryDTO.builder().sql("create table LOADER_TEST (id int, name VARCHAR2(50), xmlColumn xmltype)").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("comment on table LOADER_TEST is 'table comment中文'").build();
        client.executeSqlWithoutResultSet(source, queryDTO);

        queryDTO = SqlQueryDTO.builder().sql("comment on column LOADER_TEST.id is '中文id'").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("comment on column LOADER_TEST.name is '中文name'").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("comment on column LOADER_TEST.xmlColumn is '中文xmlColumn'").build();
        client.executeSqlWithoutResultSet(source, queryDTO);

        queryDTO = SqlQueryDTO.builder().sql("insert into LOADER_TEST values (1, 'LOADER_TEST中文', '<?xml version=\"1.0\" encoding=\"UTF-8\"?><configuration/>')").build();
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
        columnMetaDTO.setSchema("ORACLE");
        columnMetaDTO.setTableName("LOADER_TEST");
        columnMetaDTO.setColumnComment("comment");
        columnMetaDTO.setColumnName("age");
        columnMetaDTO.setColumnType("int");
        client.upsertTableColumn(source, columnMetaDTO);
    }

}
