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
import com.dtstack.dtcenter.loader.dto.source.SqlserverSourceDTO;
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
 * @Author ：LOADER_TEST
 * @Date ：Created in 04:10 2020/2/29
 * @Description：SQLServer 测试
 */
public class SQLServerTest extends BaseTest {
    // 获取数据源 client
    private static final IClient client = ClientCache.getClient(DataSourceType.SQLServer.getVal());

    private static final SqlserverSourceDTO source = SqlserverSourceDTO.builder()
            .url("jdbc:sqlserver://172.16.101.246:1433;databaseName=TestDB")
            .username("sa")
            .password("d@efaX4Wp10")
            .poolConfig(PoolConfig.builder().build())
            .build();


    @Test
    public void executeQuery1() {
        String sql = "SELECT sys.tables.name FROM sys.tables LEFT JOIN sys.schemas ON sys.tables.schema_id=sys.schemas.schema_id WHERE sys.tables.is_tracked_by_cdc = 1 AND sys.schemas.name = 'test'";
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql(sql).build();
        List<Map<String, Object>> result = client.executeQuery(source, queryDTO);
        Assert.assertTrue(CollectionUtils.isNotEmpty(result));
    }

    @BeforeClass
    public static void beforeClass() {
        // 对表禁用CDC(变更数据捕获)功能
        String cdcCloseSql = "EXEC sys.sp_cdc_disable_table  " +
                "@source_schema = 'dbo', " +
                "@source_name = 'LOADER_TEST', " +
                "@capture_instance = 'all' ";
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql(cdcCloseSql).build();
        try {
            client.executeSqlWithoutResultSet(source, queryDTO);
        } catch (Exception e) {
            // 不做处理
        }
        queryDTO = SqlQueryDTO.builder().sql("drop table if exists LOADER_TEST").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("create table LOADER_TEST (id int, name varchar(50))").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("insert into LOADER_TEST values (1, '中文LOADER_TEST')").build();
        client.executeSqlWithoutResultSet(source, queryDTO);

        queryDTO = SqlQueryDTO.builder().sql("drop view if exists loader_test_view").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("create view loader_test_view as select * from LOADER_TEST").build();
        client.executeSqlWithoutResultSet(source, queryDTO);

        // 添加表注释
        String commentSql = "exec sp_addextendedproperty N'MS_Description', N'中文_comment', N'SCHEMA', N'dbo', N'TABLE', N'LOADER_TEST'";
        queryDTO = SqlQueryDTO.builder().sql(commentSql).build();
        client.executeSqlWithoutResultSet(source, queryDTO);

        String fieldCommentSql = "EXEC sp_addextendedproperty N'MS_Description', N'id主键 自增长', N'SCHEMA', N'dbo',N'TABLE', N'LOADER_TEST', N'COLUMN', N'id'";
        queryDTO = SqlQueryDTO.builder().sql(fieldCommentSql).build();
        client.executeSqlWithoutResultSet(source, queryDTO);

        fieldCommentSql = "EXEC sp_addextendedproperty N'MS_Description', N'name 中文', N'SCHEMA', N'dbo',N'TABLE', N'LOADER_TEST', N'COLUMN', N'name'";
        queryDTO = SqlQueryDTO.builder().sql(fieldCommentSql).build();
        client.executeSqlWithoutResultSet(source, queryDTO);


        // 对表启用CDC(变更数据捕获)功能
        String cdcOpenSql = "EXEC sys.sp_cdc_enable_table " +
                        "@source_schema = 'dbo', " +
                        "@source_name = 'LOADER_TEST', " +
                        "@role_name = NULL, " +
                        "@supports_net_changes = 0 ";
        queryDTO = SqlQueryDTO.builder().sql(cdcOpenSql).build();
        try {
            client.executeSqlWithoutResultSet(source, queryDTO);
        } catch (Exception e) {
            // 不做处理
        }
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

    @Test
    public void getTableListBySchema_001() {
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().build();
        List<String> tableList = client.getTableListBySchema(source, queryDTO);
        Assert.assertTrue(CollectionUtils.isNotEmpty(tableList));
    }

    @Test
    public void getTableListBySchema_002() {
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().schema("dbo").build();
        List<String> tableList = client.getTableListBySchema(source, queryDTO);
        Assert.assertTrue(CollectionUtils.isNotEmpty(tableList));
    }

    @Test
    public void getTableListBySchema_003() {
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().schema("dbo").tableNamePattern("d").limit(1).build();
        List<String> tableList = client.getTableListBySchema(source, queryDTO);
        Assert.assertTrue(CollectionUtils.isNotEmpty(tableList));
    }

    @Test
    public void getTableListBySchema_004() {
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableNamePattern("d").limit(1).build();
        List<String> tableList = client.getTableListBySchema(source, queryDTO);
        Assert.assertTrue(CollectionUtils.isNotEmpty(tableList));
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
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("select 1").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
    }

    /**
     * 获取表列表
     */
    @Test
    public void getTableList() {
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().view(true).tableNamePattern("loader_test").build();
        List<String> tableList = client.getTableList(source, queryDTO);
        Assert.assertTrue(CollectionUtils.isNotEmpty(tableList));
    }

    /**
     * 获取表列表
     */
    @Test
    public void getTableList_view() {
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().view(true).tableNamePattern("loader_test_view").build();
        List<String> tableList = client.getTableList(source, queryDTO);
        Assert.assertEquals("[dbo].[loader_test_view]", tableList.get(0));
    }

    /**
     * 根据schema获取表 ps：该方法只能获取到开启cdc的表
     */
    @Test
    public void getTableListBySchema() {
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().schema("dbo").build();
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
        Assert.assertEquals("java.lang.Integer", columnClassInfo.get(0));
        Assert.assertEquals("java.lang.String", columnClassInfo.get(1));
        Assert.assertTrue(CollectionUtils.isNotEmpty(columnClassInfo));
    }

    /**
     * 获取表字段信息
     */
    @Test
    public void getColumnMetaData() {
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("LOADER_TEST").build();
        List<ColumnMetaDTO> columnMetaData = client.getColumnMetaData(source, queryDTO);
        Assert.assertEquals("int", columnMetaData.get(0).getType());
        Assert.assertEquals("varchar", columnMetaData.get(1).getType());
        Assert.assertTrue(CollectionUtils.isNotEmpty(columnMetaData));
    }

    /**
     * 获取表字段信息
     */
    @Test
    public void getColumnMetaData_1() {
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("[dbo].[LOADER_TEST]").build();
        List<ColumnMetaDTO> columnMetaData = client.getColumnMetaData(source, queryDTO);
        Assert.assertEquals("int", columnMetaData.get(0).getType());
        Assert.assertEquals("varchar", columnMetaData.get(1).getType());
        Assert.assertTrue(CollectionUtils.isNotEmpty(columnMetaData));
    }

    /**
     * 获取表注释
     */
    @Test
    public void getTableMetaComment() {
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("LOADER_TEST").build();
        String metaComment = client.getTableMetaComment(source, queryDTO);
        Assert.assertEquals("中文_comment", metaComment);
        Assert.assertTrue(StringUtils.isNotBlank(metaComment));
    }

    /**
     * 获取表注释
     */
    @Test
    public void getTableMetaComment_1() {
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("[dbo].[LOADER_TEST]").build();
        String metaComment = client.getTableMetaComment(source, queryDTO);
        Assert.assertEquals("中文_comment", metaComment);
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
        while (!downloader.reachedEnd()){
            List<List<String>> result = (List<List<String>>)downloader.readNext();
            for (List<String> row : result){
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
        Assert.assertTrue(CollectionUtils.isNotEmpty(client.getAllDatabases(source,queryDTO)));
    }

    /**
     * 获取正在使用的database
     */
    @Test
    public void getCurrentDatabase() {
        String currentDatabase = client.getCurrentDatabase(source);
        Assert.assertTrue(StringUtils.isNotBlank(currentDatabase));
    }
    /**
     * 获取版本
     */
    @Test
    public void getVersion() {
        Assert.assertTrue(StringUtils.isNotBlank(client.getVersion(source)));
    }

}
