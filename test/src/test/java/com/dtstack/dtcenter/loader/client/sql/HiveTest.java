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
import com.dtstack.dtcenter.loader.dto.source.HiveSourceDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import lombok.extern.slf4j.Slf4j;
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
 * @Author ：loader_test_1
 * @Date ：Created in 00:13 2020/2/29
 * @Description：Hive 测试
 */
@Slf4j
public class HiveTest extends BaseTest {

    /**
     * 构造hive客户端
     */
    private static final IClient client = ClientCache.getClient(DataSourceType.HIVE.getVal());

    /**
     * 构建数据源信息
     */
    private static final HiveSourceDTO source = HiveSourceDTO.builder()
            .url("jdbc:hive2://kudu3:10000/default")
            .schema("default")
            .defaultFS("hdfs://ns1")
            .username("admin")
            .config("{\n" +
                    "            \"dfs.ha.namenodes.ns1\" : \"nn1,nn2\",\n" +
                    "            \"dfs.namenode.rpc-address.ns1.nn2\" : \"kudu2:9000\",\n" +
                    "            \"dfs.client.failover.proxy.provider.ns1\" : \"org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider\",\n" +
                    "            \"dfs.namenode.rpc-address.ns1.nn1\" : \"kudu1:9000\",\n" +
                    "            \"dfs.nameservices\" : \"ns1\"\n" +
                    "          }")
            .poolConfig(PoolConfig.builder().build())
            .build();

    /**
     * 数据准备
     */
    //@BeforeClass
    public static void beforeClass()  {
        System.setProperty("HADOOP_USER_NAME", "admin");
        IClient client = ClientCache.getClient(DataSourceType.HIVE.getVal());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("drop table if exists loader_test_1").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("create table loader_test_1 (id int comment 'id comment', name string) COMMENT 'table comment' row format delimited fields terminated by ','").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("insert into loader_test_1 values (1, 'loader_test_1')").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("drop table if exists loader_test_parquet").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("create table loader_test_parquet (id int comment 'ID', name string comment '姓名_name') STORED AS PARQUET").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("insert into loader_test_parquet values (1, 'wc1'),(2,'wc2')").build();
        client.executeSqlWithoutResultSet(source, queryDTO);

        queryDTO = SqlQueryDTO.builder().sql("drop table if exists loader_test_downloader").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("create table loader_test_downloader (id1 SMALLINT,\n" +
                "id2 INT,\n" +
                "id3 DOUBLE,\n" +
                "id4 STRING,\n" +
                "id5 TIMESTAMP,\n" +
                "id6 DECIMAL(22,2), \n" +
                "id7 VARCHAR(100),\n" +
                "id8 CHAR(10),\n" +
                "id9 DATE,\n" +
                "id10 BINARY,\n" +
                "id11 BIGINT,\n" +
                "id12 BOOLEAN,\n" +
                "id13 FLOAT,\n" +
                "id14 TINYINT) STORED AS PARQUET").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("insert into loader_test_downloader select 4, 100, 30.25,'loader_test',current_timestamp(), 120.01,'varchar', 'char column', '2019-05-22', '0101010000101',1234567,true,90.2,1").build();
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

    @Test
    public void getDownloader_01() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.HIVE.getVal());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("loader_test_downloader").build();
        IDownloader downloader = client.getDownloader(source, queryDTO);
        Assert.assertTrue(CollectionUtils.isNotEmpty(downloader.getMetaInfo()));
        while (!downloader.reachedEnd()) {
            System.out.println(downloader.readNext());
        }
    }

    /**
     * 获取连接测试
     */
    @Test
    public void getConWithTaskParams() throws Exception {
        client.executeQuery(source, SqlQueryDTO.builder().sql("drop table if exists loader_nonstrict").build());
        client.executeQuery(source, SqlQueryDTO.builder().sql("create table if not exists loader_nonstrict (id int) partitioned by(name string)").build());
        Connection con = client.getCon(source, "hive.exec.dynamic.partition.mode=nonstrict\n" +
                "loader.age=2\n" +
                "loader.null=");
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

    @Test
    public void getTableListBySchema() {
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().schema("default").build();
        List<String> tableList = client.getTableListBySchema(source, queryDTO);
        Assert.assertTrue(CollectionUtils.isNotEmpty(tableList));
    }


    /**
     * 获取表字段 java 规范化类型
     */
    @Test
    public void getColumnClassInfo()  {
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("loader_test_1").build();
        List<String> columnClassInfo = client.getColumnClassInfo(source, queryDTO);
        Assert.assertTrue(CollectionUtils.isNotEmpty(columnClassInfo));
    }

    /**
     * 获取表字段详细信息
     */
    @Test
    public void getColumnMetaData()  {
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("loader_test_1").build();
        List<ColumnMetaDTO> columnMetaData = client.getColumnMetaData(source, queryDTO);
        Assert.assertEquals("int", columnMetaData.get(0).getType());
        Assert.assertEquals("string", columnMetaData.get(1).getType());
        Assert.assertTrue(CollectionUtils.isNotEmpty(columnMetaData));
    }

    /**
     * 获取表注释
     */
    @Test
    public void getTableMetaComment()  {
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("loader_test_1").build();
        String comment =  client.getTableMetaComment(source, queryDTO);
        Assert.assertEquals("table comment", comment);
    }

    @Test
    public void getDownloader() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.HIVE.getVal());
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
       Assert.assertTrue(CollectionUtils.isEmpty(downloader.getContainers()));
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
        IClient client = ClientCache.getClient(DataSourceType.HIVE.getVal());
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
     * 判断表 Location
     */
    @Test
    public void getTableLocation()  {
        Table table = client.getTable(source, SqlQueryDTO.builder().tableName("loader_test_1").build());
        Assert.assertNotNull(table.getPath());
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
            assert client.createDatabase(source, "loader_test", "测试注释");
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
     * 获取表详细信息，表分隔符为 制表符情况
     */
    @Test
    public void getTableTest() {
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("drop table if exists loader_test_issue_120").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("create table loader_test_issue_120 (id int, name string) row format delimited fields terminated by '    '").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
        Table table = client.getTable(source, SqlQueryDTO.builder().tableName("loader_test_issue_120").build());
        Assert.assertEquals("    ", table.getDelim());
    }

    /**
     * 获取数据库详细信息
     */
    @Test
    public void getDatabase() {
        Database database = client.getDatabase(source, "loader_test_db");
        Assert.assertEquals(database.getDbName(), "loader_test_db");
        Assert.assertEquals(database.getComment(), "test_comment");
        Assert.assertEquals(database.getOwnerName(), "admin");
        Assert.assertTrue(StringUtils.isNotBlank(database.getLocation()));
    }
}
