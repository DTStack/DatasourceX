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
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.source.PrestoSourceDTO;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.sql.Connection;
import java.util.List;

/**
 * presto inceptor 测试
 *
 * @author ：qianyi
 * date：Created in 上午9:50 2021/3/23
 * company: www.dtstack.com
 */
@Ignore
public class PrestoInceptorTest extends BaseTest {

    // 获取数据源 client
    private static final IClient client = ClientCache.getClient(DataSourceType.Presto.getVal());

    // 构建数据源信息, 测试库
    private static final PrestoSourceDTO source = PrestoSourceDTO.builder()
            .url("jdbc:presto://172.16.23.196:8080/inceptorsql")
            .username("root")
            .poolConfig(PoolConfig.builder().build())
            .build();
    /**
     * 获取连接测试
     */
    @Test
    public void getCon() throws Exception {
        Connection connection = client.getCon(source);
        Assert.assertNotNull(connection);
        connection.close();
    }

    @Test
    public void getTableList() {
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().schema("default").build();
        List<String> tableList = client.getTableListBySchema(source, queryDTO);
        tableList.forEach(table-> {
            client.getColumnMetaData(source, SqlQueryDTO.builder().schema("default").tableName(table).build());
        });
    }

    @Test
    public void getColumnMetaData() {
        client.executeQuery(source, SqlQueryDTO.builder().sql("DESCRIBE default.dl_user").build() );
        client.getColumnMetaData(source, SqlQueryDTO.builder().schema("default").tableName("dl_user").build());
    }
}
