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

import com.dtstack.dtcenter.loader.cache.pool.config.PoolConfig;
import com.dtstack.dtcenter.loader.client.BaseTest;
import com.dtstack.dtcenter.loader.client.ClientCache;
import com.dtstack.dtcenter.loader.client.IClient;
import com.dtstack.dtcenter.loader.client.ITable;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.UpsertColumnMetaDTO;
import com.dtstack.dtcenter.loader.dto.source.Greenplum6SourceDTO;
import com.dtstack.dtcenter.loader.enums.CommandType;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * greenplum table测试
 *
 * @author ：wangchuan
 * date：Created in 10:14 上午 2020/12/7
 * company: www.dtstack.com
 */
public class GreenplumTableTest extends BaseTest {

    // 获取数据源 client
    private static final ITable client = ClientCache.getTable(DataSourceType.GREENPLUM6.getVal());

    private static final Greenplum6SourceDTO source = Greenplum6SourceDTO.builder()
            .url("jdbc:pivotal:greenplum://172.16.100.186:5432;DatabaseName=postgres")
            .username("gpadmin")
            .password("gpadmin")
            .schema("public")
            .poolConfig(new PoolConfig())
            .build();

    @BeforeClass
    public static void setUp () {
        IClient client = ClientCache.getClient(DataSourceType.GREENPLUM6.getVal());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("drop view if exists gp_test_view").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("drop table if exists gp_test").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("create table gp_test (id integer, name text)").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("comment on column loader_test.id is 'id'").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("comment on column loader_test.name is '名字'").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("insert into gp_test values (1, 'gpp')").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("create view gp_test_view as select * from gp_test").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
    }

    /**
     * 数据准备
     */
    @BeforeClass
    public static void beforeClass() {
        client.executeSqlWithoutResultSet(source, "drop table if exists loader_test");
        client.executeSqlWithoutResultSet(source, "create table loader_test (id int, name text)");
        client.executeSqlWithoutResultSet(source, "comment on table loader_test is 'table comment'");
        client.executeSqlWithoutResultSet(source, "insert into loader_test values (1, 'nanqi')");
        client.executeSqlWithoutResultSet(source, "drop table if exists loader_test_2");
        client.executeSqlWithoutResultSet(source, "create table loader_test_2 (id int, name text)");
    }

    /**
     * 重命名表
     */
    @Test
    public void renameTable () {
        ITable client = ClientCache.getTable(DataSourceType.GREENPLUM6.getVal());
        Boolean renameCheck1 = client.renameTable(source, "loader_test_2", "loader_test_3");
        Assert.assertTrue(renameCheck1);
        Boolean renameCheck2 = client.renameTable(source, "loader_test_3", "loader_test_2");
        Assert.assertTrue(renameCheck2);
    }

    /**
     * 获取表大小
     */
    @Test
    public void getTableSize () {
        ITable tableClient = ClientCache.getTable(DataSourceType.GREENPLUM6.getVal());
        Long tableSize = tableClient.getTableSize(source, "public", "loader_test");
        System.out.println(tableSize);
        Assert.assertTrue(tableSize != null && tableSize > 0);
    }

    /**
     * 判断表是否是视图 - 是
     */
    @Test
    public void tableIsView () {
        ITable client = ClientCache.getTable(DataSourceType.GREENPLUM6.getVal());
        Boolean check = client.isView(source, null, "gp_test_view");
        Assert.assertTrue(check);
    }

    /**
     * 判断表是否是视图 - 否
     */
    @Test
    public void tableIsNotView () {
        ITable client = ClientCache.getTable(DataSourceType.GREENPLUM6.getVal());
        Boolean check = client.isView(source, null, "gp_test");
        Assert.assertFalse(check);
    }

    @Test
    public void upsertTableColumn() {
        UpsertColumnMetaDTO columnMetaDTO = new UpsertColumnMetaDTO();
        columnMetaDTO.setCommandType(CommandType.INSERT);
        columnMetaDTO.setSchema("public");
        columnMetaDTO.setTableName("gp_test");
        columnMetaDTO.setColumnComment("comment");
        columnMetaDTO.setColumnName("age");
        columnMetaDTO.setColumnType("int");
        client.upsertTableColumn(source, columnMetaDTO);
    }
}
