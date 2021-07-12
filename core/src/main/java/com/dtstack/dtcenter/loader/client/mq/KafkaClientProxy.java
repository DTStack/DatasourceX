package com.dtstack.dtcenter.loader.client.mq;

import com.dtstack.dtcenter.loader.ClassLoaderCallBackMethod;
import com.dtstack.dtcenter.loader.client.IKafka;
import com.dtstack.dtcenter.loader.dto.KafkaConsumerDTO;
import com.dtstack.dtcenter.loader.dto.KafkaOffsetDTO;
import com.dtstack.dtcenter.loader.dto.KafkaPartitionDTO;
import com.dtstack.dtcenter.loader.dto.KafkaTopicDTO;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

/**
 * @company: www.dtstack.com
 * @Author ：wangchuan
 * @Date ：Created in 下午2:23 2020/6/2
 * @Description：kafka代理
 */
@Slf4j
public class KafkaClientProxy<T> implements IKafka<T> {

    private IKafka targetClient;

    public KafkaClientProxy(IKafka targetClient) {
        this.targetClient = targetClient;
    }

    @Override
    public Boolean testCon(ISourceDTO source) {
        return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.testCon(source),
                targetClient.getClass().getClassLoader());
    }

    @Override
    public String getAllBrokersAddress(ISourceDTO source) {
        return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.getAllBrokersAddress(source),
                targetClient.getClass().getClassLoader());
    }

    @Override
    public List<String> getTopicList(ISourceDTO source) {
        return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.getTopicList(source),
                targetClient.getClass().getClassLoader());
    }

    @Override
    public Boolean createTopic(ISourceDTO source, KafkaTopicDTO kafkaTopic) {
        return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.createTopic(source, kafkaTopic),
                targetClient.getClass().getClassLoader());
    }

    @Override
    public List<T> getAllPartitions(ISourceDTO source, String topic) {
        return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.getAllPartitions(source, topic),
                targetClient.getClass().getClassLoader());
    }

    @Override
    public List<KafkaOffsetDTO> getOffset(ISourceDTO source, String topic) {
        return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.getOffset(source, topic),
                targetClient.getClass().getClassLoader());
    }

    @Override
    public List<List<Object>> getPreview(ISourceDTO source, SqlQueryDTO queryDTO) {
        return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.getPreview(source, queryDTO),
                targetClient.getClass().getClassLoader());
    }

    @Override
    public List<List<Object>> getPreview(ISourceDTO source, SqlQueryDTO queryDTO, String prevMode) {
        return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.getPreview(source, queryDTO, prevMode),
                targetClient.getClass().getClassLoader());
    }

    @Override
    public List<KafkaPartitionDTO> getTopicPartitions(ISourceDTO source, String topic) {
        return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.getTopicPartitions(source, topic),
                targetClient.getClass().getClassLoader());
    }

    @Override
    public List<String> consumeData(ISourceDTO source, String topic, Integer collectNum, String offsetReset, Long timestampOffset, Integer maxTimeWait) {
        return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.consumeData(source, topic, collectNum, offsetReset, timestampOffset, maxTimeWait),
                targetClient.getClass().getClassLoader());
    }

    @Override
    public List<String> listConsumerGroup(ISourceDTO source) {
        return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.listConsumerGroup(source),
                targetClient.getClass().getClassLoader());
    }

    @Override
    public List<String> listConsumerGroupByTopic(ISourceDTO source, String topic) {
        return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.listConsumerGroupByTopic(source, topic),
                targetClient.getClass().getClassLoader());
    }

    @Override
    public List<KafkaConsumerDTO> getGroupInfoByGroupId(ISourceDTO source, String groupId) {
        return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.getGroupInfoByGroupId(source, groupId),
                targetClient.getClass().getClassLoader());
    }

    @Override
    public List<KafkaConsumerDTO> getGroupInfoByTopic(ISourceDTO source, String topic) {
        return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.getGroupInfoByTopic(source, topic),
                targetClient.getClass().getClassLoader());
    }

    @Override
    public List<KafkaConsumerDTO> getGroupInfoByGroupIdAndTopic(ISourceDTO source, String groupId, String topic) {
        return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.getGroupInfoByGroupIdAndTopic(source, groupId, topic),
                targetClient.getClass().getClassLoader());
    }
}
