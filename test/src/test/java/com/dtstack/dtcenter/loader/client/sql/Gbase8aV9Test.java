package com.dtstack.dtcenter.loader.client.sql;

import com.dtstack.dtcenter.loader.client.BaseTest;
import com.dtstack.dtcenter.loader.client.ClientCache;
import com.dtstack.dtcenter.loader.client.IClient;
import com.dtstack.dtcenter.loader.dto.ColumnMetaDTO;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.source.GBaseSourceDTO;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @company: www.dtstack.com
 * @Author ：loader_test
 * @Date ：Created in 11:06 2020/4/16
 * @Description：gbase 8a 测试
 */
public class Gbase8aV9Test extends BaseTest {
    private static GBaseSourceDTO source = GBaseSourceDTO.builder()
            .url("jdbc:gbase://172.16.8.49:5258/gbase?rewriteBatchedStatements=true")
            .schema("test")
            .username("root")
            .build();

    private static IClient client = ClientCache.getClient(DataSourceType.GBase_8a.getVal());

    @BeforeClass
    public static void beforeClass() {
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("drop table if exists loader_test").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("create table loader_test (id int COMMENT 'ID', name varchar(50) COMMENT '名称') comment 'table comment'").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("insert into loader_test values (1, 'loader_test')").build();
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

    /**
     * 连通性测试
     */
    @Test
    public void testCon() {
        Boolean isConnected = client.testCon(source);
        Assert.assertTrue(isConnected);
    }

    /**
     * 预编译查询
     */
    @Test
    public void executeQuery() {
        String sql = "select * from loader_test where id > ? and id < ?;";
        List<Object> preFields = new ArrayList<>();
        preFields.add(0);
        preFields.add(5);
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql(sql).preFields(preFields).build();
        List<Map<String, Object>> result = client.executeQuery(source, queryDTO);
        Assert.assertTrue(CollectionUtils.isNotEmpty(result));
    }

    /**
     * 无需结果查询
     */
    @Test
    public void executeSqlWithoutResultSet() {
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("show tables").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
    }

    /**
     * 获取表列表
     */
    @Test
    public void getTableList() {
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().build();
        List<String> tableList = client.getTableList(source, queryDTO);
        Assert.assertTrue(CollectionUtils.isNotEmpty(tableList));
    }

    /**
     * 获取表字段java标准格式
     */
    @Test
    public void getColumnClassInfo() {
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("loader_test").build();
        List<String> columnClassInfo = client.getColumnClassInfo(source, queryDTO);
        Assert.assertTrue(CollectionUtils.isNotEmpty(columnClassInfo));
    }

    /**
     * 获取表字段信息
     */
    @Test
    public void getColumnMetaData() {
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("loader_test").build();
        List<ColumnMetaDTO> columnMetaData = client.getColumnMetaData(source, queryDTO);
        Assert.assertTrue(CollectionUtils.isNotEmpty(columnMetaData));
    }

    /**
     * 根据自定义sql获取表字段信息
     */
    @Test
    public void getColumnMetaDataWithSql() {
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("select * from loader_test").build();
        List sql = client.getColumnMetaDataWithSql(source, queryDTO);
        Assert.assertTrue(CollectionUtils.isNotEmpty(sql));
    }

    /**
     * 获取版本
     */
    @Test
    public void getVersion() {
        Assert.assertTrue(StringUtils.isNotBlank(client.getVersion(source)));
    }
}
