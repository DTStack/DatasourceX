package com.dtstack.dtcenter.loader.client.sql;

import com.dtstack.dtcenter.loader.client.BaseTest;
import com.dtstack.dtcenter.loader.client.ClientCache;
import com.dtstack.dtcenter.loader.client.IClient;
import com.dtstack.dtcenter.loader.dto.ColumnMetaDTO;
import com.dtstack.dtcenter.loader.dto.SSLConfigDTO;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.source.TrinoSourceDTO;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import com.google.common.collect.Maps;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * trino kerberos 数据源客户端测试
 *
 * @author ：wangchuan
 * date：Created in 上午9:50 2021/3/23
 * company: www.dtstack.com
 */
public class TrinoKerberosTest extends BaseTest {

    // 获取数据源 client
    private static final IClient CLIENT = ClientCache.getClient(DataSourceType.TRINO.getVal());

    // 构建数据源信息
    private static final TrinoSourceDTO SOURCE_DTO = TrinoSourceDTO.builder()
            .url("jdbc:trino://kbhadoop03:8443/hive")
            .schema("default")
            .build();

    /**
     * 数据预处理
     */
    @BeforeClass
    public static void beforeClass() {
        SSLConfigDTO configDTO = SSLConfigDTO.builder()
                .SSLKeyStorePath("/Users/wangbin/Downloads/trino-kerberos/ca/keystore")
                .SSLKeyStorePassword("abc123").build();
        Map<String, Object> kerberosMap = Maps.newHashMap();
        kerberosMap.put("principal", "trino/kbhadoop03@DTSTACK.COM");
        kerberosMap.put("principalFile", "/Users/wangbin/Downloads/trino-kerberos/trino.keytab");
        kerberosMap.put("java.security.krb5.conf", "/Users/wangbin/Downloads/trino-kerberos/krb5.conf");
        SOURCE_DTO.setSslConfigDTO(configDTO);
        SOURCE_DTO.setKerberosConfig(kerberosMap);
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("drop table if exists LOADER_TEST").build();
        CLIENT.executeSqlWithoutResultSet(SOURCE_DTO, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("create table LOADER_TEST (id int COMMENT 'id', name varchar COMMENT '姓名', pt varchar COMMENT '分区') comment 'table comment' WITH (format = 'parquet',partitioned_by= array['pt'])").build();
        CLIENT.executeSqlWithoutResultSet(SOURCE_DTO, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("insert into LOADER_TEST values (1, 'LOADER_TEST', 'pt1')").build();
        CLIENT.executeSqlWithoutResultSet(SOURCE_DTO, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("drop view if exists LOADER_TEST_VIEW").build();
        CLIENT.executeSqlWithoutResultSet(SOURCE_DTO, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("create view LOADER_TEST_VIEW as select * from LOADER_TEST").build();
        CLIENT.executeSqlWithoutResultSet(SOURCE_DTO, queryDTO);
    }

    /**
     * 获取连接测试
     */
    @Test
    public void getCon() throws Exception {
        Connection connection = CLIENT.getCon(SOURCE_DTO);
        Assert.assertNotNull(connection);
        connection.close();
    }

    /**
     * presto 获取分区字段
     * @throws Exception
     */
    @Test
    public void getColumnMetaData_part() {
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().schema("default").tableName("LOADER_TEST").build();
        List<ColumnMetaDTO> list = CLIENT.getColumnMetaData(SOURCE_DTO, queryDTO);
        Assert.assertTrue(list.size() > 0);
        for (ColumnMetaDTO columnMetaDTO : list) {
            if("pt".equals(columnMetaDTO.getKey())) {
                Assert.assertTrue(columnMetaDTO.getPart());
            }
        }

    }

    /**
     * 连通性测试
     */
    @Test
    public void testCon() {
        Boolean isConnected = CLIENT.testCon(SOURCE_DTO);
        Assert.assertTrue(isConnected);
    }

    /**
     * 预编译查询
     */
    @Test
    public void executeQuery() {
        String sql = "select * from LOADER_TEST where id > ? and id < ?";
        List<Object> preFields = new ArrayList<>();
        preFields.add(0);
        preFields.add(5);
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql(sql).preFields(preFields).build();
        List<Map<String, Object>> result = CLIENT.executeQuery(SOURCE_DTO, queryDTO);
        Assert.assertTrue(CollectionUtils.isNotEmpty(result));
    }

    /**
     * 字段别名测试
     */
    @Test
    public void executeQueryAlias() {
        String sql = "select id as testAlias from LOADER_TEST";
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql(sql).build();
        List<Map<String, Object>> result = CLIENT.executeQuery(SOURCE_DTO, queryDTO);
        Assert.assertTrue(result.get(0).containsKey("testAlias"));
    }

    /**
     * 无需结果查询
     */
    @Test
    public void executeSqlWithoutResultSet() {
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("show tables from default").build();
        CLIENT.executeSqlWithoutResultSet(SOURCE_DTO, queryDTO);
    }

    /**
     * 获取表列表 目标数据源表太多了.....
     */
    @Test
    public void getTableList() {
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().build();
        List<String> tableList = CLIENT.getTableList(SOURCE_DTO, queryDTO);
        Assert.assertTrue(CollectionUtils.isNotEmpty(tableList));
    }

    /**
     * 根据schema获取表
     */
    @Test
    public void getTableListBySchema() {
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().schema("default").build();
        List<String> tableList = CLIENT.getTableListBySchema(SOURCE_DTO, queryDTO);
        Assert.assertTrue(CollectionUtils.isNotEmpty(tableList));
    }

    /**
     * 获取表字段java标准格式
     */
    @Test
    public void getColumnClassInfo() {
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("\"LOADER_TEST\"").build();
        List<String> columnClassInfo = CLIENT.getColumnClassInfo(SOURCE_DTO, queryDTO);
        Assert.assertTrue(CollectionUtils.isNotEmpty(columnClassInfo));
    }

    /**
     * 获取表字段信息
     */
    @Test
    public void getColumnMetaData() {
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("\"LOADER_TEST\"").build();
        List<ColumnMetaDTO> columnMetaData = CLIENT.getColumnMetaData(SOURCE_DTO, queryDTO);
        List<ColumnMetaDTO> columnMetaData1 = CLIENT.getColumnMetaData(SOURCE_DTO,  SqlQueryDTO.builder().schema("default").tableName("LOADER_TEST").build());
        Assert.assertTrue(CollectionUtils.isNotEmpty(columnMetaData));
        Assert.assertTrue(CollectionUtils.isNotEmpty(columnMetaData1));
    }

    /**
     * 数据预览测试
     */
    @Test
    public void testGetPreview() {
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("\"LOADER_TEST\"").build();
        List preview = CLIENT.getPreview(SOURCE_DTO, queryDTO);
        Assert.assertTrue(CollectionUtils.isNotEmpty(preview));
    }

    /**
     * 根据自定义sql获取表字段信息
     */
    @Test
    public void getColumnMetaDataWithSql() {
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("select * from LOADER_TEST").build();
        List sql = CLIENT.getColumnMetaDataWithSql(SOURCE_DTO, queryDTO);
        Assert.assertTrue(CollectionUtils.isNotEmpty(sql));
    }

    /**
     * 获取所有的db
     */
    @Test
    public void getAllDatabases() {
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().build();
        Assert.assertTrue(CollectionUtils.isNotEmpty(CLIENT.getAllDatabases(SOURCE_DTO, queryDTO)));
    }

    /**
     * 获取表建表语句
     */
    @Test
    public void getCreateTableSql() {
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("\"LOADER_TEST\"").build();
        String createTableSql = CLIENT.getCreateTableSql(SOURCE_DTO, queryDTO);
        Assert.assertTrue(StringUtils.isNotBlank(createTableSql));
    }

    /**
     * 根据 schema 获取表
     */
    @Test
    public void getTableBySchema() {
        List tableListBySchema = CLIENT.getTableListBySchema(SOURCE_DTO, SqlQueryDTO.builder().view(true).tableNamePattern("loader").limit(20).build());
        Assert.assertTrue(CollectionUtils.isNotEmpty(tableListBySchema));
        Assert.assertTrue(tableListBySchema.contains("loader_test"));
        Assert.assertTrue(tableListBySchema.contains("loader_test_view"));
    }

    @Test
    public void isDatabaseExists() {
        assert CLIENT.isDatabaseExists(SOURCE_DTO, "default");
    }

    @Test
    public void isDatabaseNotExists() {
        assert !CLIENT.isDatabaseExists(SOURCE_DTO, UUID.randomUUID().toString());
    }

    @Test
    public void isTableExistsInDatabase() {
        assert CLIENT.isTableExistsInDatabase(SOURCE_DTO, "loader_test", "default");
    }

    @Test
    public void getCatalogs() {
        List<String> catalogs = CLIENT.getCatalogs(SOURCE_DTO);
        Assert.assertTrue(CollectionUtils.isNotEmpty(catalogs));
    }
}
