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
import com.dtstack.dtcenter.loader.dto.source.Mysql5SourceDTO;
import com.dtstack.dtcenter.loader.dto.source.PostgresqlSourceDTO;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import org.junit.Assert;
import org.junit.Test;

/**
 * bug描述：调用两个不同数据源，并且都开启连接池，在调用第二个的时候会报错
 *
 * bug链接：http://zenpms.dtstack.cn/zentao/bug-view-37213.html
 *
 * bug原因：2021-04-14 去除了 HikariDataSource 的 driverClassName 导致
 *
 * @author ：wangchuan
 * date：Created in 下午7:35 2021/5/14
 * company: www.dtstack.com
 */
public class Issue37213Test extends BaseTest {

    // 获取数据源 client
    private static final IClient MYSQL_CLIENT = ClientCache.getClient(DataSourceType.MySQL.getVal());

    // 构建数据源信息
    private static final Mysql5SourceDTO MYSQL_5_SOURCE_DTO = Mysql5SourceDTO.builder()
            .url("jdbc:mysql://172.16.100.186:3306/dev")
            .username("dev")
            .password("Abc12345")
            .poolConfig(PoolConfig.builder().build())
            .build();

    // 获取数据源 client
    private static final IClient PG_CLIENT = ClientCache.getClient(DataSourceType.PostgreSQL.getVal());

    private static final PostgresqlSourceDTO POSTGRESQL_SOURCE_DTO = PostgresqlSourceDTO.builder()
            .url("jdbc:postgresql://172.16.101.246:5432/postgres?currentSchema=public")
            .username("postgres")
            .password("abc123")
            .schema("public")
            .poolConfig(new PoolConfig())
            .build();

    /**
     * 连通性测试
     */
    @Test
    public void testCon() {
        Boolean isConnected1 = MYSQL_CLIENT.testCon(MYSQL_5_SOURCE_DTO);
        Boolean isConnected2 = PG_CLIENT.testCon(POSTGRESQL_SOURCE_DTO);
        Assert.assertTrue(isConnected1);
        Assert.assertTrue(isConnected2);
    }
}