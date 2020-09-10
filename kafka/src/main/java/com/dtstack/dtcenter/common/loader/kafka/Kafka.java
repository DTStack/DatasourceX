package com.dtstack.dtcenter.common.loader.kafka;

import com.dtstack.dtcenter.common.loader.kafka.util.KakfaUtil;
import com.dtstack.dtcenter.loader.client.IKafka;
import com.dtstack.dtcenter.loader.dto.KafkaTopicDTO;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;
import com.dtstack.dtcenter.loader.dto.source.KafkaSourceDTO;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @company: www.dtstack.com
 * @Author ：wangchuan
 * @Date ：Created in 16:08 2020/6/2
 * @Description：Kafka 客户端 支持 Kafka 0.9、0.10、0.11、1.x版本
 */
public class Kafka<T> implements IKafka<T> {
    @Override
    public Boolean testCon(ISourceDTO iSource) {
        KafkaSourceDTO kafkaSourceDTO = (KafkaSourceDTO) iSource;
        return KakfaUtil.checkConnection(kafkaSourceDTO.getUrl(), kafkaSourceDTO.getBrokerUrls(),
                kafkaSourceDTO.getKerberosConfig());
    }

    @Override
    public String getAllBrokersAddress(ISourceDTO iSource) {
        KafkaSourceDTO kafkaSourceDTO = (KafkaSourceDTO) iSource;
        if (StringUtils.isNotBlank(kafkaSourceDTO.getBrokerUrls())) {
            return kafkaSourceDTO.getBrokerUrls();
        }
        return KakfaUtil.getAllBrokersAddressFromZk(kafkaSourceDTO.getUrl());
    }

    @Override
    public List<String> getTopicList(ISourceDTO iSource) {
        List<String> topics;
        KafkaSourceDTO kafkaSourceDTO = (KafkaSourceDTO) iSource;

        if (StringUtils.isNotBlank(kafkaSourceDTO.getBrokerUrls())) {
            topics = KakfaUtil.getTopicListFromBroker(kafkaSourceDTO.getBrokerUrls(),
                    kafkaSourceDTO.getKerberosConfig());
        } else {
            topics = KakfaUtil.getTopicListFromZk(kafkaSourceDTO.getUrl());
        }

        //过滤特殊topic
        if (CollectionUtils.isNotEmpty(topics)) {
            topics = topics.stream().filter(s -> !"__consumer_offsets".equals(s)).collect(Collectors.toList());
        }

        return topics;
    }

    @Override
    public Boolean createTopic(ISourceDTO iSource, KafkaTopicDTO kafkaTopic) {
        KafkaSourceDTO kafkaSourceDTO = (KafkaSourceDTO) iSource;
        String brokerUrl = StringUtils.isBlank(kafkaSourceDTO.getBrokerUrls()) ? getAllBrokersAddress(iSource) : kafkaSourceDTO.getBrokerUrls();
        KakfaUtil.createTopicFromBroker(brokerUrl, kafkaSourceDTO.getKerberosConfig(), kafkaTopic.getTopicName(),
                kafkaTopic.getPartitions(),
                kafkaTopic.getReplicationFactor());
        return true;
    }

    @Override
    public List<T> getAllPartitions(ISourceDTO iSource, String topic) {
        KafkaSourceDTO kafkaSourceDTO = (KafkaSourceDTO) iSource;
        return (List<T>) KakfaUtil.getAllPartitionsFromZk(kafkaSourceDTO.getUrl(), topic);
    }

    @Override
    public List getOffset(ISourceDTO iSource, String topic) {
        KafkaSourceDTO kafkaSourceDTO = (KafkaSourceDTO) iSource;
        if (StringUtils.isBlank(kafkaSourceDTO.getBrokerUrls())) {
            kafkaSourceDTO.setBrokerUrls(KakfaUtil.getAllBrokersAddressFromZk(kafkaSourceDTO.getUrl()));
        }
        return KakfaUtil.getPartitionOffset(kafkaSourceDTO.getBrokerUrls(), kafkaSourceDTO.getKerberosConfig(), topic);
    }

    @Override
    public List<List<Object>> getPreview(ISourceDTO iSource, SqlQueryDTO queryDTO) {
        KafkaSourceDTO kafkaSourceDTO = (KafkaSourceDTO) iSource;
        List<String> recordsFromKafka = KakfaUtil.getRecordsFromKafka(kafkaSourceDTO.getUrl(),
                kafkaSourceDTO.getBrokerUrls(), queryDTO.getTableName(), null, kafkaSourceDTO.getKerberosConfig());
        List<Object> records = new ArrayList<>(recordsFromKafka);
        List<List<Object>> result = new ArrayList<>();
        result.add(records);
        return result;
    }
}
