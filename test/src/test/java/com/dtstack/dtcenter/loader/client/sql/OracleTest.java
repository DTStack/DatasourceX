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
import com.dtstack.dtcenter.loader.client.ITable;
import com.dtstack.dtcenter.loader.dto.ColumnMetaDTO;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.source.OracleSourceDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.util.List;
import java.util.Map;

/**
 * @company: www.dtstack.com
 * @Author ：LOADER_TEST
 * @Date ：Created in 03:54 2020/2/29
 * @Description：Oracle 测试
 */
public class OracleTest extends BaseTest {

    // 构造客户端
    private static final IClient client = ClientCache.getClient(DataSourceType.Oracle.getVal());

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

    /**
     * 测试连通性测试
     */
    @Test
    public void testCon()  {
        Boolean isConnected = client.testCon(source);
        if (Boolean.FALSE.equals(isConnected)) {
            throw new DtLoaderException("connection exception");
        }
    }

    /**
     * 执行查询语句测试
     */
    @Test
    public void executeQuery()  {
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("select count(1) from LOADER_TEST").build();
        List<Map<String, Object>> result = client.executeQuery(source, queryDTO);
        Assert.assertTrue(CollectionUtils.isNotEmpty(result));
    }

    /**
     * oracle xmlType字段预览
     */
    @Test
    public void xmlPreview()  {
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("LOADER_TEST").build();
        List previewData = client.getPreview(source, queryDTO);
        Assert.assertTrue(CollectionUtils.isNotEmpty(previewData));
    }

    /**
     * 字段别名查询测试
     */
    @Test
    public void executeQueryAlias()  {
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("select id as tAlias from LOADER_TEST").build();
        List<Map<String, Object>> result = client.executeQuery(source, queryDTO);
        Assert.assertTrue(CollectionUtils.isNotEmpty(result));
    }

    /**
     * 无结果查询测试
     */
    @Test
    public void executeSqlWithoutResultSet()  {
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("select count(1) from LOADER_TEST").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
    }

    /**
     * 获取表
     */
    @Test
    public void getTableList()  {
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().fetchSize(100).build();
        for (int i =0; i< 20 ; i++) {
            Long start = System.currentTimeMillis();
            client.getTableList(source, queryDTO);
            System.out.println(System.currentTimeMillis() - start);
        }
    }

    /**
     * 根据 schema获取表
     */
    @Test
    public void getTableListBySchema()  {
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().schema("ORACLE").build();
        List<String> tableList = client.getTableListBySchema(source, queryDTO);
        Assert.assertTrue(CollectionUtils.isNotEmpty(tableList));
    }

    /**
     * 获取java 标准字段属性
     */
    @Test
    public void getColumnClassInfo()  {
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("LOADER_TEST").build();
        List<String> columnClassInfo = client.getColumnClassInfo(source, queryDTO);
        Assert.assertTrue(CollectionUtils.isNotEmpty(columnClassInfo));
    }

    /**
     * 获取表字段详细信息
     */
    @Test
    public void getColumnMetaData()  {
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("LOADER_TEST").build();
        List<ColumnMetaDTO> columnMetaData = client.getColumnMetaData(source, queryDTO);
        assert CollectionUtils.isNotEmpty(columnMetaData);
    }

    /**
     * 获取表注释
     */
    @Test
    public void getTableMetaComment()  {
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("LOADER_TEST").build();
        client.getTableMetaComment(source, queryDTO);
    }

    /**
     * 数据预览测试
     */
    @Test
    public void testGetPreview() {
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("LOADER_TEST").previewNum(1).build();
        List preview = client.getPreview(source, queryDTO);
        Assert.assertTrue(CollectionUtils.isNotEmpty(preview));
    }

    /**
     * 指定sql downloader下载 测试
     */
    @Test
    public void downloader()throws Exception {
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
     * 获取FlinkX需要的字段类型 - 会进行一层字段类型转换，此处表中有xml类型，不支持测试
     */
    @Test(expected = DtLoaderException.class)
    public void getFlinkColumnMetaData()  {
        List metaData = client.getFlinkColumnMetaData(source, SqlQueryDTO.builder().tableName("LOADER_TEST").build());
        Assert.assertTrue(CollectionUtils.isNotEmpty(metaData));
    }

    /**
     * 根据sql 获取对应结果的字段信息
     */
    @Test
    public void getColumnMetaDataWithSql() {
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("select * from LOADER_TEST ").build();
        List result = client.getColumnMetaDataWithSql(source, queryDTO);
        Assert.assertTrue(CollectionUtils.isNotEmpty(result));
    }

    /**
     * 获取所有的schema
     */
    @Test
    public void getAllDatabases()  {
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().build();
        List databases = client.getAllDatabases(source, queryDTO);
        Assert.assertTrue(CollectionUtils.isNotEmpty(databases));
    }

    /**
     * 获取 表的建表语句 (数据源有问题)
     */
    @Test
    public void getCreateTableSql()  {
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("LOADER_TEST").build();
        String createTableSql = client.getCreateTableSql(source, queryDTO);
        Assert.assertTrue(StringUtils.isNotBlank(createTableSql));
    }

    /**
     * 获取当前使用的 schema
     */
    @Test
    public void getCurrentDatabase()  {
        String currentDatabase = client.getCurrentDatabase(source);
        Assert.assertNotNull(currentDatabase);
    }

    /**
     * 模糊查询表，不指定schema，限制条数
     */
    @Test
    public void searchTableByNameLimit ()  {
        IClient client = ClientCache.getClient(DataSourceType.Oracle.getVal());
        List list = client.getTableListBySchema(source, SqlQueryDTO.builder().view(true).tableNamePattern("TEST").limit(5).build());
        Assert.assertTrue(CollectionUtils.isNotEmpty(list));
    }

    /**
     * 模糊查询表，不指定schema，不获取视图，限制条数
     */
    @Test
    public void searchTableByNameLimitNoView ()  {
        IClient client = ClientCache.getClient(DataSourceType.Oracle.getVal());
        List list = client.getTableListBySchema(source, SqlQueryDTO.builder().view(false).tableNamePattern(" ").limit(100).build());
        Assert.assertTrue(CollectionUtils.isNotEmpty(list));
    }

    /**
     * 获取指定schema下的表，模糊查询，限制条数
     */
    @Test
    public void searchTableBySchemaLimit ()  {
        IClient client = ClientCache.getClient(DataSourceType.Oracle.getVal());
        List list = client.getTableListBySchema(source, SqlQueryDTO.builder().view(true).schema("ORACLE").tableNamePattern("TEST").limit(3).build());
        Assert.assertTrue(CollectionUtils.isNotEmpty(list));
    }

    /**
     * 获取指定schema下的表，模糊查询，不限制条数
     */
    @Test
    public void searchTableBySchema ()  {
        IClient client = ClientCache.getClient(DataSourceType.Oracle.getVal());
        List list = client.getTableListBySchema(source, SqlQueryDTO.builder().view(true).schema("ORACLE").tableNamePattern("TEST").build());
        Assert.assertTrue(CollectionUtils.isNotEmpty(list));
    }

    /**
     * 获取指定schema下的表，模糊查询，查询条件为空字符，不限制条数
     */
    @Test
    public void searchTableBySchemaEmptyName ()  {
        IClient client = ClientCache.getClient(DataSourceType.Oracle.getVal());
        List list = client.getTableListBySchema(source, SqlQueryDTO.builder().view(true).schema("ORACLE").tableNamePattern(" ").build());
        Assert.assertTrue(CollectionUtils.isNotEmpty(list));
    }

    /**
     * 获取指定schema下的表，不获取视图，限制条数
     */
    @Test
    public void searchTableAndViewBySchema ()  {
        IClient client = ClientCache.getClient(DataSourceType.Oracle.getVal());
        List list = client.getTableListBySchema(source, SqlQueryDTO.builder().view(false).schema("ORACLE").tableNamePattern("TEST").limit(200).build());
        Assert.assertTrue(CollectionUtils.isNotEmpty(list));
    }

    /**
     * 获取表占用存储
     */
    @Test
    public void getTableSize ()  {
        ITable tableClient = ClientCache.getTable(DataSourceType.Oracle.getVal());
        Long tableSize = tableClient.getTableSize(source, null, "LOADER_TEST");
        Assert.assertTrue(tableSize != null && tableSize > 0);
    }

    /**
     * 获取版本
     */
    @Test
    public void getVersion() {
        Assert.assertTrue(StringUtils.isNotBlank(client.getVersion(source)));
    }
}
