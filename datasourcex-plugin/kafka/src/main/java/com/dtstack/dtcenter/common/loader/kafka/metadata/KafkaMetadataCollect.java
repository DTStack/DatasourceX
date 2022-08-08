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

package com.dtstack.dtcenter.common.loader.kafka.metadata;

import com.dtstack.dtcenter.common.loader.kafka.util.KafkaUtil;
import com.dtstack.dtcenter.common.loader.metadata.collect.AbstractMetaDataCollect;
import com.dtstack.dtcenter.common.loader.metadata.core.MetadataBaseCollectSplit;
import com.dtstack.dtcenter.loader.dto.KafkaConsumerDTO;
import com.dtstack.dtcenter.loader.dto.metadata.MetadataCollectCondition;
import com.dtstack.dtcenter.loader.dto.metadata.entity.MetadataEntity;
import com.dtstack.dtcenter.loader.dto.metadata.entity.kafka.GroupInfo;
import com.dtstack.dtcenter.loader.dto.metadata.entity.kafka.MetadataKafkaEntity;
import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;
import com.dtstack.dtcenter.loader.dto.source.KafkaSourceDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.exception.ExceptionUtils;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;

/**
 * kafka 元数据拾取器
 *
 * @author by zhiyi
 * @date 2022/4/8 1:48 下午
 */
@Slf4j
public class KafkaMetadataCollect extends AbstractMetaDataCollect {

    private static final String KEY_PARTITIONS = "partitions";

    private static final String KEY_REPLICAS = "replicas";

    private static final String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss";

    /**
     * topic list
     */
    private List<String> topicList;

    /**
     * topic集合迭代器
     */
    private Iterator<String> iterator;

    private KafkaSourceDTO kafkaSourceDTO;

    @Override
    public void init(ISourceDTO sourceDTO, MetadataCollectCondition metadataCollectCondition, MetadataBaseCollectSplit metadataBaseCollectSplit, Queue<MetadataEntity> metadataEntities) {
        log.info("init collect Split start: {} ", metadataBaseCollectSplit);
        topicList = metadataBaseCollectSplit.getTopicList();
        this.sourceDTO = sourceDTO;
        this.kafkaSourceDTO = (KafkaSourceDTO) sourceDTO;
        this.metadataCollectCondition = metadataCollectCondition;
        this.metadataBaseCollectSplit = metadataBaseCollectSplit;
        super.metadataEntities = metadataEntities;
        this.iterator = topicList.iterator();
        IS_INIT.set(true);
        log.info("init collect Split end : {}", metadataBaseCollectSplit);
    }

    @Override
    protected void openConnection() {
        if (CollectionUtils.isEmpty(topicList)){
            throw new DtLoaderException("this kafka don't have any topics");
        }
    }

    @Override
    public MetadataEntity readNext() {
        String currentTopic = iterator.next();
        MetadataKafkaEntity metadatakafkaEntity = new MetadataKafkaEntity();
        try {
            metadatakafkaEntity = queryMetadata(currentTopic);
            metadatakafkaEntity.setQuerySuccess(true);
        } catch (Exception e) {
            log.error("kafka collect error: {}", ExceptionUtils.getRootCauseMessage(e), e);
            metadatakafkaEntity.setQuerySuccess(false);
            metadatakafkaEntity.setErrorMsg("Caused by: " + e.getCause().getClass() + ":" + e.getCause().getMessage());
        }
        return metadatakafkaEntity;
    }

    /**
     * 执行查询操作
     *
     * @param topic topic
     * @return kafak元数据实体类
     */
    public MetadataKafkaEntity queryMetadata(String topic) throws Exception {
        MetadataKafkaEntity entity = new MetadataKafkaEntity();
        entity.setTopicName(topic);
        Map<String, Integer> countAndReplicas = KafkaUtil.getTopicPartitionCountAndReplicas(kafkaSourceDTO, topic);
        entity.setPartitions(countAndReplicas.get(KEY_PARTITIONS));
        entity.setReplicationFactor(countAndReplicas.get(KEY_REPLICAS));

        SimpleDateFormat sdf = new SimpleDateFormat();
        sdf.applyPattern(DATE_FORMAT);
        entity.setTimeStamp(System.currentTimeMillis());

        List<String> groups = KafkaUtil.listConsumerGroup(kafkaSourceDTO, topic);
        List<GroupInfo> groupInfos = new ArrayList<>();
        if (CollectionUtils.isNotEmpty(groups)) {
            for (String group : groups) {
                GroupInfo groupInfo = new GroupInfo();
                List<KafkaConsumerDTO> infos = KafkaUtil.getGroupInfoByGroupId(kafkaSourceDTO, group, topic);
                groupInfo.setGroupId(group);
                groupInfo.setTopic(topic);
                groupInfo.setPartitionInfo(infos);
                groupInfos.add(groupInfo);
            }
        } else {
            GroupInfo groupInfo = new GroupInfo();
            List<KafkaConsumerDTO> infos = KafkaUtil.getGroupInfoByGroupId(kafkaSourceDTO, "", topic);
            groupInfo.setTopic(topic);
            groupInfo.setPartitionInfo(infos);
            groupInfos.add(groupInfo);
        }
        entity.setGroupInfo(groupInfos);
        return entity;
    }

    @Override
    public boolean reachEnd() {
        return !this.iterator.hasNext();
    }

    @Override
    protected MetadataEntity createMetadataEntity() {
        return null;
    }

    @Override
    protected List<Object> showTables() {
        return null;
    }

    @Override
    public void close() {

    }
}
