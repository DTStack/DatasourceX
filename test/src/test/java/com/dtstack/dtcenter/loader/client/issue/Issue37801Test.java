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

package com.dtstack.dtcenter.loader.client.issue;

import com.dtstack.dtcenter.loader.cache.pool.config.PoolConfig;
import com.dtstack.dtcenter.loader.client.BaseTest;
import com.dtstack.dtcenter.loader.client.ClientCache;
import com.dtstack.dtcenter.loader.client.IClient;
import com.dtstack.dtcenter.loader.client.ITable;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.Table;
import com.dtstack.dtcenter.loader.dto.source.HiveSourceDTO;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * bug描述：将视图表识别为了非视图表
 *
 * bug链接：http://zenpms.dtstack.cn/zentao/bug-view-37801.html
 *
 * bug原因：在使用 hive、spark 执行 sql `desc formatted tableName` 时返回值中的 key 为 columnType
 * 被当成了判断视图所用的key，导致返回调用 ITable.isView 方法返回 false
 *
 * @author ：wangchuan
 * date：Created in 上午10:26 2021/5/31
 * company: www.dtstack.com
 */
public class Issue37801Test extends BaseTest {

    /**
     * 构造hive table客户端
     */
    private static final ITable TABLE_CLIENT = ClientCache.getTable(DataSourceType.HIVE.getVal());

    /**
     * 构造hive table客户端
     */
    private static final IClient CLIENT = ClientCache.getClient(DataSourceType.HIVE.getVal());

    /**
     * 构建数据源信息
     */
    private static final HiveSourceDTO HIVE_SOURCE_DTO = HiveSourceDTO.builder()
            .url("jdbc:hive2://172.16.100.214:10000/default")
            .username("admin")
            .poolConfig(PoolConfig.builder().build())
            .build();

    /**
     * 数据准备
     */
    @BeforeClass
    public static void setUp () {
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("drop table if exists loader_test_37801").build();
        CLIENT.executeSqlWithoutResultSet(HIVE_SOURCE_DTO, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("create table loader_test_37801 (id int, name string)").build();
        CLIENT.executeSqlWithoutResultSet(HIVE_SOURCE_DTO, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("drop view if exists loader_test_view_37801").build();
        CLIENT.executeSqlWithoutResultSet(HIVE_SOURCE_DTO, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("create view loader_test_view_37801 as select * from loader_test_37801").build();
        CLIENT.executeSqlWithoutResultSet(HIVE_SOURCE_DTO, queryDTO);
    }

    @Test
    public void test_for_issue() {
        Assert.assertTrue(TABLE_CLIENT.isView(HIVE_SOURCE_DTO, "default", "loader_test_view_37801"));
    }

    @Test
    public void test_for_issue2() {
        Table table = CLIENT.getTable(HIVE_SOURCE_DTO, SqlQueryDTO.builder().tableName("loader_test_view_37801").build());
        Assert.assertTrue(table.getIsView());
    }
}
