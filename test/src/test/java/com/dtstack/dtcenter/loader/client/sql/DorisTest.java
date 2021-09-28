/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dtstack.dtcenter.loader.client.sql;

import com.dtstack.dtcenter.loader.IDownloader;
import com.dtstack.dtcenter.loader.cache.pool.config.PoolConfig;
import com.dtstack.dtcenter.loader.client.BaseTest;
import com.dtstack.dtcenter.loader.client.ClientCache;
import com.dtstack.dtcenter.loader.client.IClient;
import com.dtstack.dtcenter.loader.dto.ColumnMetaDTO;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.source.DorisSourceDTO;
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

public class DorisTest extends BaseTest {

    // 获取数据源 client
    private static final IClient client = ClientCache.getClient(DataSourceType.DORIS.getVal());

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
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("drop table if exists LOADER_TEST").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("create table LOADER_TEST (id int COMMENT 'id', name varchar(50) COMMENT '姓名') COMMENT \"table comment\" DISTRIBUTED BY HASH(id) BUCKETS 10").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("insert into LOADER_TEST values (1, 'LOADER_TEST')").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("drop view if exists LOADER_TEST_VIEW").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("create view LOADER_TEST_VIEW as select * from LOADER_TEST").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
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
     * 根据schema获取表
     */
    @Test
    public void getTableListBySchema_001() {
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().schema("qianyi").build();
        List<String> tableList = client.getTableListBySchema(source, queryDTO);
        Assert.assertTrue(CollectionUtils.isNotEmpty(tableList));
    }

    /**
     * 选择schema
     */
    @Test
    public void getTableList() {
        List<String> tableList = client.getTableList(source, SqlQueryDTO.builder().schema("qianyi").build());
        Assert.assertTrue(CollectionUtils.isNotEmpty(tableList));
    }

    /**
     * 选择schema
     */
    @Test
    public void getTableList_001() {
        List<String> tableList = client.getTableList(source, SqlQueryDTO.builder().build());
        Assert.assertTrue(CollectionUtils.isNotEmpty(tableList));
    }

    @Test
    public void getColumnMetaData_0001() {
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().schema("qianyi").tableName("LOADER_TEST").build();
        List<ColumnMetaDTO> columnMetaData = client.getColumnMetaData(source, queryDTO);
        Assert.assertTrue(CollectionUtils.isNotEmpty(columnMetaData));
    }


    /**
     * 获取连接测试
     */
    @Test
    public void getCon() throws Exception {
        Connection connection = client.getCon(source);
        Assert.assertNotNull(connection);
        connection.close();
    }

    /**
     * 预编译查询
     */
    @Test
    public void executeQuery() {
        String sql = "select * from LOADER_TEST where id > ? and id < ?;";
        List<Object> preFields = new ArrayList<>();
        preFields.add(0);
        preFields.add(5);
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql(sql).preFields(preFields).build();
        List<Map<String, Object>> result = client.executeQuery(source, queryDTO);
        Assert.assertTrue(CollectionUtils.isNotEmpty(result));
    }

    /**
     * 字段名称重复测试
     */
    @Test
    public void executeQueryRepeatColumn() {
        String sql = "select id, name ,id as id, name, name as name, id as name,name as id from LOADER_TEST";
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql(sql).build();
        List<Map<String, Object>> result = client.executeQuery(source, queryDTO);
        Map<String, Object> row = result.get(0);
        Assert.assertEquals(7, row.size());
        Assert.assertTrue(row.containsKey("name") && row.containsKey("name(1)")
                && row.containsKey("name(2)") && row.containsKey("name(3)")
                && row.containsKey("id") && row.containsKey("id(1)")
                && row.containsKey("id(2)"));
    }

    /**
     * 字段别名测试
     */
    @Test
    public void executeQueryAlias() {
        String sql = "select id as testAlias from LOADER_TEST;";
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql(sql).build();
        List<Map<String, Object>> result = client.executeQuery(source, queryDTO);
        Assert.assertTrue(result.get(0).containsKey("testAlias"));
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
     * 根据schema获取表
     */
    @Test
    public void getTableListBySchema() {
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().schema("qianyi").build();
        List<String> tableList = client.getTableListBySchema(source, queryDTO);
        Assert.assertTrue(CollectionUtils.isNotEmpty(tableList));
    }

    /**
     * 获取表字段java标准格式
     */
    @Test
    public void getColumnClassInfo() {
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("LOADER_TEST").build();
        List<String> columnClassInfo = client.getColumnClassInfo(source, queryDTO);
        Assert.assertTrue(CollectionUtils.isNotEmpty(columnClassInfo));
    }

    /**
     * 获取表字段信息
     */
    @Test
    public void getColumnMetaData() {
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("LOADER_TEST").build();
        List<ColumnMetaDTO> columnMetaData = client.getColumnMetaData(source, queryDTO);
        Assert.assertTrue(CollectionUtils.isNotEmpty(columnMetaData));
    }

    /**
     * 获取表注释
     */
    @Test
    public void getTableMetaComment() {
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("LOADER_TEST").build();
        String metaComment = client.getTableMetaComment(source, queryDTO);
        Assert.assertTrue(StringUtils.isNotBlank(metaComment));
    }

    /**
     * 自定义sql 数据下载测试
     */
    @Test
    public void testGetDownloader() throws Exception {
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("select * from LOADER_TEST").build();
        IDownloader downloader = client.getDownloader(source, queryDTO);
        List<String> metaInfo = downloader.getMetaInfo();
        Assert.assertTrue(CollectionUtils.isNotEmpty(metaInfo));
        while (!downloader.reachedEnd()) {
            List<List<String>> result = (List<List<String>>) downloader.readNext();
            for (List<String> row : result) {
                Assert.assertTrue(CollectionUtils.isNotEmpty(row));
            }
        }
    }

    /**
     * 数据预览测试
     */
    @Test
    public void testGetPreview() {
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("LOADER_TEST").build();
        List preview = client.getPreview(source, queryDTO);
        Assert.assertTrue(CollectionUtils.isNotEmpty(preview));
    }

    /**
     * 根据自定义sql获取表字段信息
     */
    @Test
    public void getColumnMetaDataWithSql() {
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("select * from LOADER_TEST").build();
        List sql = client.getColumnMetaDataWithSql(source, queryDTO);
        Assert.assertTrue(CollectionUtils.isNotEmpty(sql));
    }

    /**
     * 获取所有的db
     */
    @Test
    public void getAllDatabases() {
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().build();
        Assert.assertTrue(CollectionUtils.isNotEmpty(client.getAllDatabases(source, queryDTO)));
    }

    /**
     * 获取表建表语句
     */
    @Test
    public void getCreateTableSql() {
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("LOADER_TEST").build();
        String createTableSql = client.getCreateTableSql(source, queryDTO);
        Assert.assertTrue(StringUtils.isNotBlank(createTableSql));
    }

    /**
     * 获取正在使用的database
     */
    @Test
    public void getCurrentDatabase() {
        String currentDatabase = client.getCurrentDatabase(source);
        Assert.assertEquals("qianyi", currentDatabase);
    }

    /**
     * 根据 schema 获取表
     */
    @Test
    public void getTableBySchema() {
        List tableListBySchema = client.getTableListBySchema(source, SqlQueryDTO.builder().schema("qianyi").tableNamePattern("").limit(5).build());
        Assert.assertTrue(CollectionUtils.isNotEmpty(tableListBySchema));
    }

    /**
     * 获取downloader
     */
    @Test
    public void getDownloader() throws Exception {
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("LOADER_TEST").sql("select * from LOADER_TEST").build();
        IDownloader iDownloader = client.getDownloader(source, queryDTO);
        List<String> list = iDownloader.getMetaInfo();
        Assert.assertTrue(CollectionUtils.isNotEmpty(list));
    }

    @Test
    public void isDatabaseExists() {
        assert client.isDatabaseExists(source, "qianyi");
    }

    @Test
    public void isTableExistsInDatabase() {
        assert client.isTableExistsInDatabase(source, "LOADER_TEST", "qianyi");
    }

    /**
     * 获取表 - 无视图
     */
    @Test
    public void tableListNotView() {
        List tableListBySchema = client.getTableListBySchema(source, SqlQueryDTO.builder().schema("qianyi").tableNamePattern("").limit(5).build());
        Assert.assertFalse(tableListBySchema.contains("LOADER_TEST_VIEW"));
    }

    /**
     * 获取表 - 有视图
     */
    @Test
    public void tableListContainView() {
        List tableListBySchema = client.getTableListBySchema(source, SqlQueryDTO.builder().schema("qianyi").tableNamePattern("").view(true).limit(10).build());
        Assert.assertTrue(tableListBySchema.contains("LOADER_TEST_VIEW"));
    }

    /**
     * 获取版本
     */
    @Test
    public void getVersion() {
        Assert.assertTrue(StringUtils.isNotBlank(client.getVersion(source)));
    }
}
