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

package com.dtstack.dtcenter.common.loader.confluent5;

import com.dtstack.dtcenter.common.loader.common.exception.ErrorCode;
import com.dtstack.dtcenter.common.loader.confluent5.util.Confluent5Util;
import com.dtstack.dtcenter.loader.client.IKafka;
import com.dtstack.dtcenter.loader.dto.KafkaConsumerDTO;
import com.dtstack.dtcenter.loader.dto.KafkaPartitionDTO;
import com.dtstack.dtcenter.loader.dto.KafkaTopicDTO;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.source.Confluent5SourceDTO;
import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * <p>confluent 客户端</p>
 * <p>支持 kerberos 认证、用户密码认证、 ssl 认证、 schema registry</>
 *
 * @author ：wangchuan
 * date：Created in 上午10:21 2022/2/18
 * company: www.dtstack.com
 */
public class Confluent5 implements IKafka {
    @Override
    public Boolean testCon(ISourceDTO sourceDTO) {
        Confluent5SourceDTO confluent5SourceDTO = (Confluent5SourceDTO) sourceDTO;
        return Confluent5Util.checkConnection(confluent5SourceDTO);
    }

    @Override
    public String getAllBrokersAddress(ISourceDTO sourceDTO) {
        Confluent5SourceDTO confluent5SourceDTO = (Confluent5SourceDTO) sourceDTO;
        if (StringUtils.isNotBlank(confluent5SourceDTO.getBrokerUrls())) {
            return confluent5SourceDTO.getBrokerUrls();
        }
        return Confluent5Util.getAllBrokersAddressFromZk(confluent5SourceDTO.getUrl());
    }

    @Override
    public List<String> getTopicList(ISourceDTO sourceDTO) {
        Confluent5SourceDTO confluent5SourceDTO = (Confluent5SourceDTO) sourceDTO;
        List<String> topics = Confluent5Util.getTopicList(confluent5SourceDTO);
        // 过滤掉存储消费者组 offset 的 topic
        return topics.stream().filter(s -> !"__consumer_offsets".equals(s)).collect(Collectors.toList());
    }

    @Override
    public Boolean createTopic(ISourceDTO sourceDTO, KafkaTopicDTO kafkaTopic) {
        Confluent5SourceDTO confluent5SourceDTO = (Confluent5SourceDTO) sourceDTO;
        Confluent5Util.createTopicFromBroker(confluent5SourceDTO, kafkaTopic.getTopicName(), kafkaTopic.getPartitions(), kafkaTopic.getReplicationFactor());
        return true;
    }

    @Override
    public List getOffset(ISourceDTO sourceDTO, String topic) {
        Confluent5SourceDTO confluent5SourceDTO = (Confluent5SourceDTO) sourceDTO;
        return Confluent5Util.getPartitionOffset(confluent5SourceDTO, topic);
    }

    @Override
    public List<List<Object>> getPreview(ISourceDTO sourceDTO, SqlQueryDTO queryDTO) {
        return getPreview(sourceDTO, queryDTO, Confluent5Util.EARLIEST);
    }

    @Override
    public List<List<Object>> getPreview(ISourceDTO sourceDTO, SqlQueryDTO queryDTO, String prevMode) {
        Confluent5SourceDTO confluent5SourceDTO = (Confluent5SourceDTO) sourceDTO;
        List<String> recordsFromKafka = Confluent5Util.getRecordsFromKafka(confluent5SourceDTO, queryDTO.getTableName(), prevMode);
        List<Object> records = new ArrayList<>(recordsFromKafka);
        List<List<Object>> result = new ArrayList<>();
        result.add(records);
        return result;
    }

    @Override
    public List<KafkaPartitionDTO> getTopicPartitions(ISourceDTO source, String topic) {
        Confluent5SourceDTO confluent5SourceDTO = (Confluent5SourceDTO) source;
        return Confluent5Util.getPartitions(confluent5SourceDTO, topic);
    }

    @Override
    public List<String> consumeData(ISourceDTO source, String topic, Integer collectNum, String offsetReset, Long timestampOffset, Integer maxTimeWait) {
        Confluent5SourceDTO confluent5SourceDTO = (Confluent5SourceDTO) source;
        return Confluent5Util.consumeData(confluent5SourceDTO, topic, collectNum, offsetReset, timestampOffset, maxTimeWait);
    }

    @Override
    public List<String> listConsumerGroup(ISourceDTO source) {
        Confluent5SourceDTO confluent5SourceDTO = (Confluent5SourceDTO) source;
        return Confluent5Util.listConsumerGroup(confluent5SourceDTO, null);
    }

    @Override
    public List<String> listConsumerGroupByTopic(ISourceDTO source, String topic) {
        if (StringUtils.isBlank(topic)) {
            throw new DtLoaderException("topic cannot be empty...");
        }
        Confluent5SourceDTO confluent5SourceDTO = (Confluent5SourceDTO) source;
        return Confluent5Util.listConsumerGroup(confluent5SourceDTO, topic);
    }

    @Override
    public List<KafkaConsumerDTO> getGroupInfoByGroupId(ISourceDTO source, String groupId) {
        Confluent5SourceDTO confluent5SourceDTO = (Confluent5SourceDTO) source;
        return Confluent5Util.getGroupInfoByGroupId(confluent5SourceDTO, groupId, null);
    }

    @Override
    public List<KafkaConsumerDTO> getGroupInfoByTopic(ISourceDTO source, String topic) {
        if (StringUtils.isBlank(topic)) {
            throw new DtLoaderException("topic cannot be empty...");
        }
        Confluent5SourceDTO confluent5SourceDTO = (Confluent5SourceDTO) source;
        return Confluent5Util.getGroupInfoByGroupId(confluent5SourceDTO, null, topic);
    }

    @Override
    public List<KafkaConsumerDTO> getGroupInfoByGroupIdAndTopic(ISourceDTO source, String groupId, String topic) {
        if (StringUtils.isBlank(topic)) {
            throw new DtLoaderException("topic cannot be empty...");
        }
        Confluent5SourceDTO confluent5SourceDTO = (Confluent5SourceDTO) source;
        return Confluent5Util.getGroupInfoByGroupId(confluent5SourceDTO, groupId, topic);
    }
}
