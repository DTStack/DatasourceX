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
import com.dtstack.dtcenter.loader.dto.Database;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.Table;
import com.dtstack.dtcenter.loader.dto.source.SparkSourceDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 00:13 2020/2/29
 * @Description：Spark 测试
 */
public class SparkTest extends BaseTest{

    /**
     * 构造spark客户端
     */
    private static final IClient client = ClientCache.getClient(DataSourceType.Spark.getVal());

    /**
     * 构建数据源信息
     */
    private static final SparkSourceDTO source = SparkSourceDTO.builder()
            .url("jdbc:hive2://172.16.100.214:10000/default")
            .schema("default")
            .defaultFS("hdfs://ns1")
            .username("admin")
            .config("{\n" +
                    "    \"dfs.ha.namenodes.ns1\": \"nn1,nn2\",\n" +
                    "    \"dfs.namenode.rpc-address.ns1.nn2\": \"172.16.101.227:9000\",\n" +
                    "    \"dfs.client.failover.proxy.provider.ns1\": \"org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider\",\n" +
                    "    \"dfs.namenode.rpc-address.ns1.nn1\": \"172.16.101.196:9000\",\n" +
                    "    \"dfs.nameservices\": \"ns1\"\n" +
                    "}")
            .poolConfig(PoolConfig.builder().build())
            .build();

    /**
     * 数据准备
     */
    @BeforeClass
    public static void beforeClass()  {
        System.setProperty("HADOOP_USER_NAME", "admin");
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("drop table if exists loader_test_1").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("create table loader_test_1 (id int comment '中文_id', name string comment '中文_name') COMMENT '中文_comment' row format delimited fields terminated by ','").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("insert into loader_test_1 values (1, 'loader_test_1')").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("drop table if exists loader_test_parquet").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("create table loader_test_parquet (id int comment '中文_id', name string comment '中文_name') STORED AS PARQUET").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("insert into loader_test_parquet values (1, 'wc1'),(2,'wc2')").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("drop database if exists loader_test_db").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("create database loader_test_db comment 'test_comment'").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
    }

    /**
     * 获取连接测试
     */
    @Test
    public void getCon() throws Exception {
        Connection con = client.getCon(source);
        Assert.assertNotNull(con);
        con.close();
    }

    /**
     * 获取连接测试
     */
    @Test
    public void getConWithTaskParams() throws Exception {
        client.executeQuery(source, SqlQueryDTO.builder().sql("drop table if exists loader_nonstrict").build());
        client.executeQuery(source, SqlQueryDTO.builder().sql("create table if not exists loader_nonstrict (id int) partitioned by(name string)").build());
        Connection con = client.getCon(source, "hive.exec.dynamic.partition=true\nhive.exec.dynamic.partition.mode=nonstrict\nhiveconf:loader.age=2\nloader.null=");
        Statement statement = con.createStatement();
        statement.execute("insert overwrite table loader_nonstrict partition(name) select id ,name from loader_test_1");
        Assert.assertNotNull(con);
        statement.close();
        con.close();
    }

    /**
     * 连通性测试
     */
    @Test
    public void testCon()  {
        Boolean isConnected = client.testCon(source);
        if (Boolean.FALSE.equals(isConnected)) {
            throw new DtLoaderException("connection exception");
        }
    }

    /**
     * 执行简单查询
     */
    @Test
    public void executeQuery()  {
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("show tables").build();
        List<Map<String, Object>> mapList = client.executeQuery(source, queryDTO);
        Assert.assertTrue(CollectionUtils.isNotEmpty(mapList));
    }

    /**
     * 执行sql无需结果
     */
    @Test
    public void executeSqlWithoutResultSet()  {
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("show tables").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
    }

    /**
     * 获取表列表
     */
    @Test
    public void getTableList()  {
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().build();
        List<String> tableList = client.getTableList(source, queryDTO);
        Assert.assertTrue(CollectionUtils.isNotEmpty(tableList));
    }

    /**
     * 获取表字段 java 规范化类型
     */
    @Test
    public void getColumnClassInfo()  {
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("loader_test_1").build();
        List<String> columnClassInfo = client.getColumnClassInfo(source, queryDTO);
        Assert.assertEquals("java.lang.Integer", columnClassInfo.get(0));
        Assert.assertEquals("java.lang.String", columnClassInfo.get(1));
        Assert.assertTrue(CollectionUtils.isNotEmpty(columnClassInfo));
    }

    /**
     * 获取表字段详细信息
     */
    @Test
    public void getColumnMetaData()  {
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("loader_test_1").build();
        List<ColumnMetaDTO> columnMetaData = client.getColumnMetaData(source, queryDTO);
        Assert.assertEquals("中文_id", columnMetaData.get(0).getComment());
        Assert.assertEquals("中文_name", columnMetaData.get(1).getComment());
        Assert.assertTrue(CollectionUtils.isNotEmpty(columnMetaData));
    }

    /**
     * 获取表注释
     */
    @Test
    public void getTableMetaComment()  {
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("loader_test_1").build();
        client.getTableMetaComment(source, queryDTO);
    }

    @Test
    public void getDownloader() throws Exception {
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("loader_test_1").build();
        IDownloader downloader = client.getDownloader(source, queryDTO);
        Assert.assertTrue(CollectionUtils.isNotEmpty(downloader.getMetaInfo()));
        while (!downloader.reachedEnd()){
            Assert.assertNotNull(downloader.readNext());
        }
    }

    @Test
    public void getDownloaderForParquet()throws Exception {
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("loader_test_parquet").build();
        IDownloader downloader = client.getDownloader(source, queryDTO);
        Assert.assertTrue(CollectionUtils.isNotEmpty(downloader.getMetaInfo()));
        while (!downloader.reachedEnd()){
            Assert.assertNotNull(downloader.readNext());
        }
    }

    @Test
    public void getPreview() {
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("loader_test_1").build();
        List preview = client.getPreview(source, queryDTO);
        Assert.assertTrue(CollectionUtils.isNotEmpty(preview));
    }

    @Test
    public void getPartitionColumn() {
        List<ColumnMetaDTO> data = client.getColumnMetaData(source, SqlQueryDTO.builder().tableName("loader_test_1").build());
        data.forEach(x-> System.out.println(x.getKey()+"=="+x.getPart()));
    }

    @Test
    public void getPreview2() {
        HashMap<String, String> map = new HashMap<>();
        map.put("id", "1");
        List list = client.getPreview(source, SqlQueryDTO.builder().tableName("loader_test_1").partitionColumns(map).build());
        Assert.assertTrue(CollectionUtils.isNotEmpty(list));
    }

    /**
     * 简单查询
     */
    @Test
    public void query() {
        List list = client.executeQuery(source, SqlQueryDTO.builder().sql("desc formatted loader_test_1").build());
        Assert.assertTrue(CollectionUtils.isNotEmpty(list));
    }

    /**
     * 根据sql获取结果字段信息
     */
    @Test
    public void getColumnMetaDataWithSql() {
        SqlQueryDTO sqlQueryDTO = SqlQueryDTO.builder().sql("select * from loader_test_1 ").build();
        List list = client.getColumnMetaDataWithSql(source, sqlQueryDTO);
        Assert.assertTrue(CollectionUtils.isNotEmpty(list));
    }

    /**
     * 获取建表sql
     */
    @Test
    public void getCreateTableSql()  {
        SqlQueryDTO sqlQueryDTO = SqlQueryDTO.builder().tableName("loader_test_1").build();
        String createTableSql = client.getCreateTableSql(source, sqlQueryDTO);
        Assert.assertTrue(StringUtils.isNotBlank(createTableSql));
    }

    /**
     * 获取所有的库列表
     */
    @Test
    public void getAllDataBases()  {
        SqlQueryDTO sqlQueryDTO = SqlQueryDTO.builder().build();
        List databases = client.getAllDatabases(source, sqlQueryDTO);
        Assert.assertTrue(CollectionUtils.isNotEmpty(databases));
    }

    /**
     * 获取表详细信息
     */
    @Test
    public void getTable()  {
        Table table = client.getTable(source, SqlQueryDTO.builder().tableName("loader_test_1").build());
        Assert.assertNotNull(table);
    }

    /**
     * 获取正在使用的数据库
     */
    @Test
    public void getCurrentDatabase()  {
        String currentDatabase = client.getCurrentDatabase(source);
        Assert.assertNotNull(currentDatabase);
    }

    /**
     * 创建库测试
     */
    @Test
    public void createDb()  {
        try {
            client.executeSqlWithoutResultSet(source, SqlQueryDTO.builder().sql("drop database if exists loader_test").build());
            client.createDatabase(source, "loader_test", "测试注释");
        } catch (Exception e) {
            // 可能失败
        }
    }

    /**
     * 判断db是否存在
     */
    @Test
    public void isDbExists()  {
        assert client.isDatabaseExists(source, "default");
    }

    /**
     * 表在db中
     */
    @Test
    public void tableInDb()  {
        assert client.isTableExistsInDatabase(source, "loader_test_1", "default");
    }

    /**
     * 表不在db中
     */
    @Test
    public void tableNotInDb()  {
        assert !client.isTableExistsInDatabase(source, "test_n", "default");
    }

    /**
     * 获取数据库详细信息
     */
    @Test
    public void getDatabase() {
        Database database = client.getDatabase(source, "loader_test_db");
        Assert.assertEquals(database.getDbName(), "loader_test_db");
        Assert.assertEquals(database.getComment(), "test_comment");
        Assert.assertTrue(StringUtils.isNotBlank(database.getLocation()));
    }
}
