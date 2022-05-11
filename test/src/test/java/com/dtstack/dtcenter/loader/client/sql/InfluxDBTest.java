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

import com.dtstack.dtcenter.loader.client.BaseTest;
import com.dtstack.dtcenter.loader.client.ClientCache;
import com.dtstack.dtcenter.loader.client.IClient;
import com.dtstack.dtcenter.loader.dto.ColumnMetaDTO;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.source.InfluxDBSourceDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import org.apache.commons.collections.CollectionUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Map;

/**
 * influxDB 测试类
 *
 * @author ：wangchuan
 * date：Created in 上午10:33 2021/6/7
 * company: www.dtstack.com
 */
public class InfluxDBTest extends BaseTest {

    // 构建client
    private static final IClient CLIENT = ClientCache.getClient(DataSourceType.INFLUXDB.getVal());

    // 构建数据源信息
    private static final InfluxDBSourceDTO SOURCE_DTO = InfluxDBSourceDTO.builder()
            .url("http://172.16.21.76:8086")
            .database("wangchuan_test")
            .build();

    /**
     * 连通性测试
     */
    @Test
    public void testCon() {
        Boolean isConnected = CLIENT.testCon(SOURCE_DTO);
        if (Boolean.FALSE.equals(isConnected)) {
            throw new DtLoaderException("connection exception");
        }
    }

    @Test
    public void getTableList() {
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().limit(2).build();
        List<String> tableList = CLIENT.getTableList(SOURCE_DTO, queryDTO);
        Assert.assertTrue(CollectionUtils.isNotEmpty(tableList));
    }

    @Test
    public void getDatabaseList() {
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().build();
        List<String> list = CLIENT.getAllDatabases(SOURCE_DTO, queryDTO);
        Assert.assertTrue(CollectionUtils.isNotEmpty(list));
    }

    @Test
    public void getPreview() {
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("wangchuan_test").previewNum(5).build();
        List<List<Object>> preview = CLIENT.getPreview(SOURCE_DTO, queryDTO);
        Assert.assertTrue(CollectionUtils.isNotEmpty(preview) && preview.size() == 6);
    }

    @Test
    public void executorQuery() {
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("select * from wangchuan_test").build();
        List<Map<String, Object>> result = CLIENT.executeQuery(SOURCE_DTO, queryDTO);
        Assert.assertTrue(CollectionUtils.isNotEmpty(result));
    }

    @Test
    public void getColumnMetaData() {
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("wangchuan_test").build();
        List<ColumnMetaDTO> metaData = CLIENT.getColumnMetaData(SOURCE_DTO, queryDTO);
        Assert.assertTrue(CollectionUtils.isNotEmpty(metaData));
    }

    @Test
    public void createDB() {
        try {
            CLIENT.createDatabase(SOURCE_DTO, "wangchuan_test", null);
        } catch (Exception e) {
            // do nothing
        }
    }

}
