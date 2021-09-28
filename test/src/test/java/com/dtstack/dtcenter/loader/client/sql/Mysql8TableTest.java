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
import com.dtstack.dtcenter.loader.client.ITable;
import com.dtstack.dtcenter.loader.dto.source.Mysql8SourceDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class Mysql8TableTest extends BaseTest {

    /**
     * 构造mysql客户端
     */
    private static final ITable client = ClientCache.getTable(DataSourceType.MySQL.getVal());

    // 构建数据源信息
    private static final Mysql8SourceDTO source = Mysql8SourceDTO.builder()
            .url("jdbc:mysql://172.16.100.186:3306/dev")
            .username("dev")
            .password("Abc12345")
            .poolConfig(PoolConfig.builder().build())
            .build();

    @Test(expected = DtLoaderException.class)
    public void showPartitions() {
        client.showPartitions(source, "dev");
    }

    /**
     * 更改表相关参数，暂时只支持更改表注释
     */
    @Test
    public void alterTableParams() {
        Map<String,String> param = new HashMap<>();
        param.put("comment","aaa");
        client.alterTableParams(source, "LOADER_TEST", param);
    }

}
