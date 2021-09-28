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

package com.dtstack.dtcenter.loader.client;

import com.dtstack.dtcenter.loader.dto.KafkaConsumerDTO;
import com.dtstack.dtcenter.loader.dto.KafkaOffsetDTO;
import com.dtstack.dtcenter.loader.dto.KafkaPartitionDTO;
import com.dtstack.dtcenter.loader.dto.KafkaTopicDTO;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;

import java.util.List;

/**
 * @company: www.dtstack.com
 * @Author ：wangchuan
 * @Date ：Created in 下午2:08 2020/6/2
 * @Description：kafka客户端接口
 */
public interface IKafka<T> {

    /**
     * 校验 连接
     *
     * @param source
     * @return
     * @throws Exception
     */
    Boolean testCon(ISourceDTO source);

    /**
     * 获取所有 Brokers 的地址
     *
     * @param source
     * @return
     * @throws Exception
     */
    String getAllBrokersAddress(ISourceDTO source);

    /**
     * 获取 所有 Topic 信息
     *
     * @param source
     * @return
     * @throws Exception
     */
    List<String> getTopicList(ISourceDTO source);

    /**
     * 创建 Topic
     *
     * @param source
     * @param kafkaTopic
     * @return
     * @throws Exception
     */
    Boolean createTopic(ISourceDTO source, KafkaTopicDTO kafkaTopic);

    /**
     * 获取特定 Topic 分区信息
     *
     * @param source
     * @param topic
     * @return
     * @throws Exception
     */
    @Deprecated
    List<T> getAllPartitions(ISourceDTO source, String topic);

    /**
     * 获取特定 Topic 所有分区的偏移量
     *
     * @param source
     * @param topic
     * @return
     * @throws Exception
     */
    List<KafkaOffsetDTO> getOffset(ISourceDTO source, String topic);

    /**
     * 获取预览数据
     * @param source
     * @param queryDTO
     * @deprecated since 1.4.0 in favor of
     * {@link #getPreview(ISourceDTO source, SqlQueryDTO queryDTO, String prevMode)}
     * @return
     * @throws Exception
     */
    @Deprecated
    List<List<Object>> getPreview(ISourceDTO source, SqlQueryDTO queryDTO);

    /**
     * 获取预览数据
     * @param source 数据源信息
     * @param queryDTO 查询条件
     * @param prevMode 预览模式
     * @return
     * @throws Exception
     */
    List<List<Object>> getPreview(ISourceDTO source, SqlQueryDTO queryDTO, String prevMode);

    /**
     * 获取kafka指定topic下的分区信息
     *
     * @param source 数据源信息
     * @param topic topic名称
     * @return 分区数量
     */
    List<KafkaPartitionDTO> getTopicPartitions (ISourceDTO source, String topic);

    /**
     * 从kafka 中消费数据
     *
     * @param source          数据源信息
     * @param topic           topic
     * @param collectNum      最大条数
     * @param offsetReset     从哪里开始消费
     * @param timestampOffset 消费启始位置
     * @param maxTimeWait     最大等待时间，单位秒
     * @return kafka数据
     */
    List<String> consumeData(ISourceDTO source, String topic, Integer collectNum, String offsetReset, Long timestampOffset, Integer maxTimeWait);

    /**
     * 获取所有的消费者组
     *
     * @param source 数据源信息
     * @return 消费者组列表
     */
    List<String> listConsumerGroup(ISourceDTO source);

    /**
     * 获取指定topic下的所有的消费者组
     *
     * @param source 数据源信息
     * @return 消费者组列表
     */
    List<String> listConsumerGroupByTopic(ISourceDTO source, String topic);

    /**
     * 获取 kafka 消费者组详细信息
     *
     * @param source  数据源信息
     * @param groupId 消费者组
     * @return 消费者组详细信息
     */
    List<KafkaConsumerDTO> getGroupInfoByGroupId(ISourceDTO source, String groupId);

    /**
     * 获取 kafka 指定topic 下消费者组详细信息
     *
     * @param source 数据源信息
     * @param topic  kafka主题
     * @return 消费者组详细信息
     */
    List<KafkaConsumerDTO> getGroupInfoByTopic(ISourceDTO source, String topic);

    /**
     * 获取 kafka 指定topic下指定消费者组详细信息
     *
     * @param source  数据源信息
     * @param groupId 消费者组
     * @param topic   kafka主题
     * @return 消费者组详细信息
     */
    List<KafkaConsumerDTO> getGroupInfoByGroupIdAndTopic(ISourceDTO source, String groupId, String topic);
}
