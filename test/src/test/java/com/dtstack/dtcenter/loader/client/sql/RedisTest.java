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
import com.dtstack.dtcenter.loader.client.IRedis;
import com.dtstack.dtcenter.loader.dto.RedisQueryDTO;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.source.RedisSourceDTO;
import com.dtstack.dtcenter.loader.enums.RedisCompareOp;
import com.dtstack.dtcenter.loader.enums.RedisDataType;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 14:03 2020/2/29
 * @Description：Redis 测试
 */
public class RedisTest extends BaseTest {
    RedisSourceDTO source = RedisSourceDTO.builder()
            .hostPort("172.16.101.246:16379")
            .password("DT@Stack#123")
            .schema("1")
            .build();


    @Test
    public void executeQuery_String() {
        IRedis client = ClientCache.getRedis(DataSourceType.REDIS.getVal());
        Map<String,Object> map = client.executeQuery(source, RedisQueryDTO.builder().redisDataType(RedisDataType.STRING).keys(Arrays.asList("loader_test_string1")).redisCompareOp(RedisCompareOp.EQUAL).build());
        assert MapUtils.isNotEmpty(map);
        map = client.executeQuery(source, RedisQueryDTO.builder().redisDataType(RedisDataType.STRING).keyLimit(3).keys(Arrays.asList("loader_test_string*")).redisCompareOp(RedisCompareOp.LIKE).build());
        assert MapUtils.isNotEmpty(map);
        map = client.executeQuery(source, RedisQueryDTO.builder().redisDataType(RedisDataType.STRING).keys(Arrays.asList("loader_test_string1","loader_test_string2","loader_test_string3")).redisCompareOp(RedisCompareOp.EQUAL).build());
        assert MapUtils.isNotEmpty(map);
    }

    @Test
    public void executeQuery_hash() {
        IRedis client = ClientCache.getRedis(DataSourceType.REDIS.getVal());
        Map<String,Object> map = client.executeQuery(source, RedisQueryDTO.builder().redisDataType(RedisDataType.HASH).keys(Arrays.asList("loader_test_hash")).redisCompareOp(RedisCompareOp.EQUAL).build());
        assert MapUtils.isNotEmpty(map);
        map = client.executeQuery(source, RedisQueryDTO.builder().redisDataType(RedisDataType.HASH).keys(Arrays.asList("loader_test_hash*")).redisCompareOp(RedisCompareOp.LIKE).build());
        assert MapUtils.isNotEmpty(map);
        map = client.executeQuery(source, RedisQueryDTO.builder().redisDataType(RedisDataType.HASH).keys(Arrays.asList("loader_test_hash1","loader_test_hash")).redisCompareOp(RedisCompareOp.EQUAL).build());
        assert MapUtils.isNotEmpty(map);
    }

    @Test
    public void executeQuery_set() {
        IRedis client = ClientCache.getRedis(DataSourceType.REDIS.getVal());
        Map<String,Object> map = client.executeQuery(source, RedisQueryDTO.builder().redisDataType(RedisDataType.SET).keys(Arrays.asList("loader_test_set")).redisCompareOp(RedisCompareOp.EQUAL).build());
        assert MapUtils.isNotEmpty(map);
        map = client.executeQuery(source, RedisQueryDTO.builder().redisDataType(RedisDataType.SET).keys(Arrays.asList("loader_test_set*")).redisCompareOp(RedisCompareOp.LIKE).build());
        assert MapUtils.isNotEmpty(map);
        map = client.executeQuery(source, RedisQueryDTO.builder().redisDataType(RedisDataType.SET).keys(Arrays.asList("loader_test_set","loader_test_set1","loader_test_set2")).redisCompareOp(RedisCompareOp.EQUAL).build());
        assert MapUtils.isNotEmpty(map);
    }

    @Test
    public void executeQuery_list() {
        IRedis client = ClientCache.getRedis(DataSourceType.REDIS.getVal());
        Map<String,Object> map = client.executeQuery(source, RedisQueryDTO.builder().redisDataType(RedisDataType.LIST).keys(Arrays.asList("loader_test_list")).redisCompareOp(RedisCompareOp.EQUAL).build());
        assert MapUtils.isNotEmpty(map);
        map = client.executeQuery(source, RedisQueryDTO.builder().redisDataType(RedisDataType.LIST).keyLimit(1).keys(Arrays.asList("loader_test_list*")).redisCompareOp(RedisCompareOp.LIKE).build());
        assert MapUtils.isNotEmpty(map);
        map = client.executeQuery(source, RedisQueryDTO.builder().redisDataType(RedisDataType.LIST).ResultLimit(1).keys(Arrays.asList("loader_test_list","loader_test_list1","loader_test_list2")).redisCompareOp(RedisCompareOp.EQUAL).build());
        assert MapUtils.isNotEmpty(map);
    }

    @Test
    public void executeQuery_zset() {
        IRedis client = ClientCache.getRedis(DataSourceType.REDIS.getVal());
        Map<String,Object> map = client.executeQuery(source, RedisQueryDTO.builder().redisDataType(RedisDataType.ZSET).keys(Arrays.asList("loader_test_zset")).redisCompareOp(RedisCompareOp.EQUAL).build());
        assert MapUtils.isNotEmpty(map);
        map = client.executeQuery(source, RedisQueryDTO.builder().redisDataType(RedisDataType.ZSET).keyLimit(1).keys(Arrays.asList("loader_test_zset*")).redisCompareOp(RedisCompareOp.LIKE).build());
        assert MapUtils.isNotEmpty(map);
        map = client.executeQuery(source, RedisQueryDTO.builder().redisDataType(RedisDataType.ZSET).keys(Arrays.asList("loader_test_zset","loader_test_zset1","loader_test_zset2")).redisCompareOp(RedisCompareOp.EQUAL).build());
        assert MapUtils.isNotEmpty(map);
    }

    @Test
    public void previewKey() {
        IRedis client = ClientCache.getRedis(DataSourceType.REDIS.getVal());
        List<String> map = client.preViewKey(source, RedisQueryDTO.builder().redisDataType(RedisDataType.ZSET).build());
        assert CollectionUtils.isNotEmpty(map);
        List<String> map1 = client.preViewKey(source, RedisQueryDTO.builder().redisDataType(RedisDataType.HASH).keyPattern("loader_test").build());
        List<String> map2 = client.preViewKey(source, RedisQueryDTO.builder().redisDataType(RedisDataType.HASH).keyPattern("loader").build());
        assert CollectionUtils.isNotEmpty(map2);
        assert map1.size() == map2.size();
    }

    /**
     * 连通性测试
     */
    @Test
    public void testCon() {
        IClient client = ClientCache.getClient(DataSourceType.REDIS.getVal());
        Boolean isConnected = client.testCon(source);
        if (Boolean.FALSE.equals(isConnected)) {
            throw new DtLoaderException("connection exception");
        }
    }

    /**
     * 数据预览测试 - 没有插入数据的方法目前
     */
    @Test
    public void preview() {
        IClient client = ClientCache.getClient(DataSourceType.REDIS.getVal());
        client.getPreview(source, SqlQueryDTO.builder().previewNum(5).tableName("loader_test").build());
    }
}
