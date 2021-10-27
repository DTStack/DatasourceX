package com.dtstack.dtcenter.common.loader.tbds.kafka;

import com.dtstack.dtcenter.common.loader.common.exception.ErrorCode;
import com.dtstack.dtcenter.common.loader.tbds.kafka.util.KafkaUtil;
import com.dtstack.dtcenter.loader.client.IKafka;
import com.dtstack.dtcenter.loader.dto.KafkaConsumerDTO;
import com.dtstack.dtcenter.loader.dto.KafkaPartitionDTO;
import com.dtstack.dtcenter.loader.dto.KafkaTopicDTO;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;
import com.dtstack.dtcenter.loader.dto.source.TbdsKafkaSourceDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * kafka 客户端，支持 kafka 0.10、10.11、1.x、2.x 版本
 * 支持 kafka kerberos认证(SASL/GSSAPI)、用户名密码认证(SASL/PLAIN)
 *
 * @author ：wangchuan
 * date：Created in 下午4:39 2021/7/9
 * company: www.dtstack.com
 */
public class Kafka<T> implements IKafka<T> {
    @Override
    public Boolean testCon(ISourceDTO sourceDTO) {
        TbdsKafkaSourceDTO kafkaSourceDTO = (TbdsKafkaSourceDTO) sourceDTO;
        return KafkaUtil.checkConnection(kafkaSourceDTO);
    }

    @Override
    public String getAllBrokersAddress(ISourceDTO sourceDTO) {
        TbdsKafkaSourceDTO kafkaSourceDTO = (TbdsKafkaSourceDTO) sourceDTO;
        if (StringUtils.isNotBlank(kafkaSourceDTO.getBrokerUrls())) {
            return kafkaSourceDTO.getBrokerUrls();
        }
        return KafkaUtil.getAllBrokersAddressFromZk(kafkaSourceDTO.getUrl());
    }

    @Override
    public List<String> getTopicList(ISourceDTO sourceDTO) {
        TbdsKafkaSourceDTO kafkaSourceDTO = (TbdsKafkaSourceDTO) sourceDTO;
        List<String> topics = KafkaUtil.getTopicList(kafkaSourceDTO);
        // 过滤掉存储消费者组 offset 的 topic
        return topics.stream().filter(s -> !"__consumer_offsets".equals(s)).collect(Collectors.toList());
    }

    @Override
    public Boolean createTopic(ISourceDTO sourceDTO, KafkaTopicDTO kafkaTopic) {
        TbdsKafkaSourceDTO kafkaSourceDTO = (TbdsKafkaSourceDTO) sourceDTO;
        KafkaUtil.createTopicFromBroker(kafkaSourceDTO, kafkaTopic.getTopicName(), kafkaTopic.getPartitions(), kafkaTopic.getReplicationFactor());
        return true;
    }

    @Override
    public List getOffset(ISourceDTO sourceDTO, String topic) {
        TbdsKafkaSourceDTO kafkaSourceDTO = (TbdsKafkaSourceDTO) sourceDTO;
        return KafkaUtil.getPartitionOffset(kafkaSourceDTO, topic);
    }

    @Override
    public List<List<Object>> getPreview(ISourceDTO sourceDTO, SqlQueryDTO queryDTO) {
        return getPreview(sourceDTO, queryDTO, KafkaUtil.EARLIEST);
    }

    @Override
    public List<List<Object>> getPreview(ISourceDTO sourceDTO, SqlQueryDTO queryDTO, String prevMode) {
        TbdsKafkaSourceDTO kafkaSourceDTO = (TbdsKafkaSourceDTO) sourceDTO;
        List<String> recordsFromKafka = KafkaUtil.getRecordsFromKafka(kafkaSourceDTO, queryDTO.getTableName(), prevMode);
        List<Object> records = new ArrayList<>(recordsFromKafka);
        List<List<Object>> result = new ArrayList<>();
        result.add(records);
        return result;
    }

    @Override
    public List<KafkaPartitionDTO> getTopicPartitions(ISourceDTO source, String topic) {
        TbdsKafkaSourceDTO kafkaSourceDTO = (TbdsKafkaSourceDTO) source;
        return KafkaUtil.getPartitions(kafkaSourceDTO, topic);
    }

    @Override
    public List<String> consumeData(ISourceDTO source, String topic, Integer collectNum, String offsetReset, Long timestampOffset, Integer maxTimeWait) {
        TbdsKafkaSourceDTO kafkaSourceDTO = (TbdsKafkaSourceDTO) source;
        return KafkaUtil.consumeData(kafkaSourceDTO, topic, collectNum, offsetReset, timestampOffset, maxTimeWait);
    }

    @Override
    public List<String> listConsumerGroup(ISourceDTO source) {
        TbdsKafkaSourceDTO kafkaSourceDTO = (TbdsKafkaSourceDTO) source;
        return KafkaUtil.listConsumerGroup(kafkaSourceDTO, null);
    }

    @Override
    public List<String> listConsumerGroupByTopic(ISourceDTO source, String topic) {
        if (StringUtils.isBlank(topic)) {
            throw new DtLoaderException("topic cannot be empty...");
        }
        TbdsKafkaSourceDTO kafkaSourceDTO = (TbdsKafkaSourceDTO) source;
        return KafkaUtil.listConsumerGroup(kafkaSourceDTO, topic);
    }

    @Override
    public List<KafkaConsumerDTO> getGroupInfoByGroupId(ISourceDTO source, String groupId) {
        TbdsKafkaSourceDTO kafkaSourceDTO = (TbdsKafkaSourceDTO) source;
        return KafkaUtil.getGroupInfoByGroupId(kafkaSourceDTO, groupId, null);
    }

    @Override
    public List<KafkaConsumerDTO> getGroupInfoByTopic(ISourceDTO source, String topic) {
        if (StringUtils.isBlank(topic)) {
            throw new DtLoaderException("topic cannot be empty...");
        }
        TbdsKafkaSourceDTO kafkaSourceDTO = (TbdsKafkaSourceDTO) source;
        return KafkaUtil.getGroupInfoByGroupId(kafkaSourceDTO, null, topic);
    }

    @Override
    public List<KafkaConsumerDTO> getGroupInfoByGroupIdAndTopic(ISourceDTO source, String groupId, String topic) {
        if (StringUtils.isBlank(topic)) {
            throw new DtLoaderException("topic cannot be empty...");
        }
        TbdsKafkaSourceDTO kafkaSourceDTO = (TbdsKafkaSourceDTO) source;
        return KafkaUtil.getGroupInfoByGroupId(kafkaSourceDTO, groupId, topic);
    }

    @Override
    public List<T> getAllPartitions(ISourceDTO source, String topic) {
        throw new DtLoaderException(ErrorCode.NOT_SUPPORT.getDesc());
    }
}
