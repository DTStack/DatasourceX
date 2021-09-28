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
import com.dtstack.dtcenter.loader.client.IKafka;
import com.dtstack.dtcenter.loader.dto.KafkaConsumerDTO;
import com.dtstack.dtcenter.loader.dto.KafkaOffsetDTO;
import com.dtstack.dtcenter.loader.dto.KafkaPartitionDTO;
import com.dtstack.dtcenter.loader.dto.KafkaTopicDTO;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.source.KafkaSourceDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import org.apache.commons.collections.CollectionUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

/**
 * kafka sasl/plain 认证
 *
 * @author ：wangchuan
 * date：Created in 下午2:04 2021/7/12
 * company: www.dtstack.com
 */
public class KafkaSaslPlainTest extends BaseTest {

    // 构建kafka数据源信息
    private static final KafkaSourceDTO source = KafkaSourceDTO.builder()
            //.url("172.16.101.233:2181/kafka")
            .brokerUrls("172.16.101.233:9092")
            .username("admin")
            .password("admin")
            .build();

    @BeforeClass
    public static void setUp() {
        createTopic();
    }

    @Test
    public void testConForKafka() {
        IKafka client = ClientCache.getKafka(DataSourceType.KAFKA_2X.getVal());
        Boolean isConnected = client.testCon(source);
        if (Boolean.FALSE.equals(isConnected)) {
            throw new DtLoaderException("connection exception");
        }
    }

    @Test
    public void testConForClient() {
        IClient client = ClientCache.getClient(DataSourceType.KAFKA_2X.getVal());
        Boolean isConnected = client.testCon(source);
        if (Boolean.FALSE.equals(isConnected)) {
            throw new DtLoaderException("connection exception");
        }
    }

    @Test
    public void getAllBrokersAddress() {
        IKafka client = ClientCache.getKafka(DataSourceType.KAFKA_2X.getVal());
        String brokersAddress = client.getAllBrokersAddress(source);
        assert (null != brokersAddress);
    }

    @Test
    public void getTopicList() {
        IKafka client = ClientCache.getKafka(DataSourceType.KAFKA_2X.getVal());
        List<String> topicList = client.getTopicList(source);
        assert (topicList != null);
        System.out.println(topicList);
    }

    public static void createTopic() {
        try {
            IKafka client = ClientCache.getKafka(DataSourceType.KAFKA_2X.getVal());
            KafkaTopicDTO topicDTO = KafkaTopicDTO.builder().partitions(3).replicationFactor((short) 1).topicName(
                    "loader_test").build();
            Boolean clientTopic = client.createTopic(source, topicDTO);
            assert (Boolean.TRUE.equals(clientTopic));
        } catch (Exception e) {
        }

    }

    @Test
    public void getOffset() {
        IKafka client = ClientCache.getKafka(DataSourceType.KAFKA_2X.getVal());
        List<KafkaOffsetDTO> offset = client.getOffset(source, "loader_test");
        assert (offset != null);
    }

    @Test
    public void testPollView(){
        IKafka client = ClientCache.getKafka(DataSourceType.KAFKA_2X.getVal());
        SqlQueryDTO sqlQueryDTO = SqlQueryDTO.builder().tableName("loader_test").build();
        List<List<Object>> results = client.getPreview(source,sqlQueryDTO);
        System.out.println(results);
    }

    @Test
    public void testPollViewLatest(){
        IKafka client = ClientCache.getKafka(DataSourceType.KAFKA_2X.getVal());
        SqlQueryDTO sqlQueryDTO = SqlQueryDTO.builder().tableName("loader_test").build();
        List<List<Object>> results = client.getPreview(source, sqlQueryDTO, "latest");
        System.out.println(results);
    }

    @Test
    public void testTopicPartitions() {
        IKafka client = ClientCache.getKafka(DataSourceType.KAFKA_2X.getVal());
        List<KafkaPartitionDTO> partitionDTOS = client.getTopicPartitions(source, "loader_test");
        Assert.assertTrue(CollectionUtils.isNotEmpty(partitionDTOS));
    }

    @Test
    public void consumeData() {
        IKafka client = ClientCache.getKafka(DataSourceType.KAFKA_2X.getVal());
        List<String> result = client.consumeData(source, "loader_test", 5, "earliest", null, 60);
        Assert.assertEquals(5, result.size());
    }

    @Test
    public void consumeDataTimestamp() {
        IKafka client = ClientCache.getKafka(DataSourceType.KAFKA_2X.getVal());
        // 从指定的timestamp 消费
        List<String> result = client.consumeData(source, "loader_test", 5, "timestamp", 1615871158731L, 60);
        Assert.assertEquals(5, result.size());
    }

    @Test
    public void listConsumerGroup() {
        IKafka client = ClientCache.getKafka(DataSourceType.KAFKA.getVal());
        Assert.assertTrue(CollectionUtils.isNotEmpty(client.listConsumerGroup(source)));
    }

    @Test
    public void listConsumerGroupByTopic() {
        IKafka client = ClientCache.getKafka(DataSourceType.KAFKA.getVal());
        Assert.assertTrue(CollectionUtils.isNotEmpty(client.listConsumerGroupByTopic(source, "loader_test")));
    }

    @Test
    public void getGroupInfoByGroupId() {
        IKafka client = ClientCache.getKafka(DataSourceType.KAFKA.getVal());
        List<KafkaConsumerDTO> info = client.getGroupInfoByGroupId(source, "test-consumer-group");
        Assert.assertTrue(CollectionUtils.isNotEmpty(info));
    }

    @Test
    public void getGroupInfoByTopic() {
        IKafka client = ClientCache.getKafka(DataSourceType.KAFKA.getVal());
        List<KafkaConsumerDTO> info = client.getGroupInfoByTopic(source, "loader_test");
        Assert.assertTrue(CollectionUtils.isNotEmpty(info));
    }

    @Test
    public void getGroupInfoByGroupIdAndTopic() {
        IKafka client = ClientCache.getKafka(DataSourceType.KAFKA.getVal());
        List<KafkaConsumerDTO> info = client.getGroupInfoByGroupIdAndTopic(source, "test-consumer-group", "loader_test");
        Assert.assertTrue(CollectionUtils.isNotEmpty(info));
    }
}
