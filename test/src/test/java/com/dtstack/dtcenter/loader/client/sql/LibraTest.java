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
import com.dtstack.dtcenter.loader.dto.source.LibraSourceDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import com.google.common.collect.Lists;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * libra client测试
 *
 * @author ：wangchuan
 * date：Created in 10:14 上午 2020/12/7
 * company: www.dtstack.com
 */
public class LibraTest extends BaseTest {

    // 获取数据源 client
    private static final IClient client = ClientCache.getClient(DataSourceType.LIBRA.getVal());

    private static final LibraSourceDTO source = LibraSourceDTO.builder()
            .url("jdbc:postgresql://172.16.101.246:5432/postgres")
            .username("postgres")
            .password("abc123")
            //.schema("public")
            .poolConfig(new PoolConfig())
            .build();

    /**
     * 数据准备
     */
    @BeforeClass
    public static void beforeClass() {
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("drop table if exists loader_test").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("create table loader_test (id int, name text)").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("comment on table loader_test is 'table comment'").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("comment on column loader_test.id is 'ID';").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("comment on column loader_test.name is '名字';").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("insert into loader_test values (1, 'nanqi')").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("drop table if exists loader_test_download").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("create table loader_test_download (id int, name text)").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
        List<String> values = Lists.newArrayList();
        // 插入 202 条数据用作 download 测试
        for (int i = 0; i < 202; i++) {
            values.add(String.format("(%s,'%s')", i, UUID.randomUUID().toString().substring(0, 20)));
        }
        queryDTO = SqlQueryDTO.builder().sql("insert into loader_test_download values " + String.join(",", values)).build();
        client.executeSqlWithoutResultSet(source, queryDTO);
        // 空表下载测试
        queryDTO = SqlQueryDTO.builder().sql("drop table if exists loader_test_download_empty").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("create table loader_test_download_empty (id int, name text)").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
    }

    /**
     * 创建库测试 - 该账号无权限
     */
    //@Test
    public void createDb() {
        assert client.createDatabase(source, "loader_test", "测试注释");
    }

    /**
     * 判断db是否存在
     */
    @Test
    public void isDbExists() {
        assert client.isDatabaseExists(source, "public");
    }

    /**
     * 判断db是否存在
     */
    @Test
    public void isDbNotExists() {
        assert !client.isDatabaseExists(source, "loader_test");
    }


    /**
     * 判断表是否在db中
     */
    @Test
    public void isTableInDb() {
        assert !client.isTableExistsInDatabase(source, "test_1", "postgres");
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
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("select count(1) from loader_test").build();
        List<Map<String, Object>> result = client.executeQuery(source, queryDTO);
        Assert.assertTrue(CollectionUtils.isNotEmpty(result));
    }

    /**
     * 字段别名查询测试
     */
    @Test
    public void executeQueryAlias()  {
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("select id as tAlias from loader_test").build();
        List<Map<String, Object>> result = client.executeQuery(source, queryDTO);
        Assert.assertTrue(CollectionUtils.isNotEmpty(result));
    }

    /**
     * 无结果查询测试
     */
    @Test
    public void executeSqlWithoutResultSet()  {
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("select count(1) from loader_test").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
    }

    /**
     * 获取表
     */
    @Test
    public void getTableList()  {
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().build();
        client.getTableList(source, queryDTO);
    }

    /**
     * 获取java 标准字段属性
     */
    @Test
    public void getColumnClassInfo()  {
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("loader_test").build();
        List<String> columnClassInfo = client.getColumnClassInfo(source, queryDTO);
        Assert.assertTrue(CollectionUtils.isNotEmpty(columnClassInfo));
    }

    /**
     * 获取表字段详细信息
     */
    @Test
    public void getColumnMetaData()  {
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("loader_test").build();
        List<ColumnMetaDTO> columnMetaData = client.getColumnMetaData(source, queryDTO);
        System.out.println(columnMetaData);
    }

    /**
     * 数据预览测试
     */
    @Test
    public void preview() {
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("loader_test").previewNum(1).build();
        List preview = client.getPreview(source, queryDTO);
        Assert.assertTrue(CollectionUtils.isNotEmpty(preview));
    }

    /**
     * 根据sql 获取对应结果的字段信息
     */
    @Test
    public void getColumnMetaDataWithSql() {
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("select * from loader_test ").build();
        List<ColumnMetaDTO> result = client.getColumnMetaDataWithSql(source, queryDTO);
        Assert.assertEquals("int4", result.get(0).getType());
        Assert.assertEquals("text", result.get(1).getType());
        Assert.assertTrue(CollectionUtils.isNotEmpty(result));
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
     * 获取当前使用的 schema
     */
    @Test
    public void getAllDb()  {
        List allDatabases = client.getAllDatabases(source, SqlQueryDTO.builder().build());
        System.out.println(allDatabases);
    }

    /**
     * 数据下载测试 - 空表
     */
    @Test
    public void downloadEmpty() throws Exception{
        IDownloader downloader = client.getDownloader(source, SqlQueryDTO.builder().sql("select * from loader_test_download_empty").build());
        int count = 0;
        while (!downloader.reachedEnd()) {
            List<List<String>> pageResult = (List<List<String>>) downloader.readNext();
            count = count + pageResult.size();
        }
        Assert.assertEquals(count, 0);
    }

    /**
     * 数据下载测试
     */
    @Test
    public void download() throws Exception{
        IDownloader downloader = client.getDownloader(source, SqlQueryDTO.builder().sql("select * from loader_test_download").build());
        int count = 0;
        while (!downloader.reachedEnd()) {
            List<List<String>> pageResult = (List<List<String>>) downloader.readNext();
            Assert.assertTrue(CollectionUtils.isNotEmpty(pageResult));
            count = count + pageResult.size();
        }
        Assert.assertEquals(count, 202);
    }

    /**
     * 获取版本
     */
    @Test
    public void getVersion() {
        Assert.assertTrue(StringUtils.isNotBlank(client.getVersion(source)));
    }
}
