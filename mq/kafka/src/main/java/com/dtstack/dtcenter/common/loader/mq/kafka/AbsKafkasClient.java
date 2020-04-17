package com.dtstack.dtcenter.common.loader.mq.kafka;

import com.dtstack.dtcenter.common.loader.kafkas.common.AbsMQClient;
import com.dtstack.dtcenter.loader.dto.KafkaTopicDTO;
import com.dtstack.dtcenter.loader.dto.SourceDTO;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 22:39 2020/2/26
 * @Description：Kafka 数据源
 */
public abstract class AbsKafkasClient<T> extends AbsMQClient<T> {

    @Override
    public Boolean testCon(SourceDTO source) {
        return KakfaUtil.checkConnection(source.getUrl(), source.getBrokerUrls(), source.getKerberosConfig());
    }

    @Override
    public String getAllBrokersAddress(SourceDTO source) throws Exception {
        if (StringUtils.isNotBlank(source.getBrokerUrls())) {
            return source.getBrokerUrls();
        }
        return KakfaUtil.getAllBrokersAddressFromZk(source.getUrl());
    }

    @Override
    public List<String> getTopicList(SourceDTO source) throws Exception {
        if (StringUtils.isNotBlank(source.getBrokerUrls())) {
            return KakfaUtil.getTopicListFromBroker(source.getBrokerUrls(), source.getKerberosConfig());
        }
        return KakfaUtil.getTopicListFromZk(source.getUrl());
    }

    @Override
    public Boolean createTopic(SourceDTO source, KafkaTopicDTO kafkaTopic) throws Exception {
        return KakfaUtil.createTopicFromZk(source.getUrl(), kafkaTopic.getTopicName(), kafkaTopic.getPartitions(),
                kafkaTopic.getReplicationFactor());
    }

    @Override
    public List<T> getAllPartitions(SourceDTO source, String topic) throws Exception {
        return (List<T>) KakfaUtil.getAllPartitionsFromZk(source.getUrl(), topic);
    }

    @Override
    public List getOffset(SourceDTO source, String topic) throws Exception {
        if (StringUtils.isBlank(source.getBrokerUrls())) {
            source.setBrokerUrls(KakfaUtil.getAllBrokersAddressFromZk(source.getUrl()));
        }
        return KakfaUtil.getPartitionOffset(source.getBrokerUrls(), source.getKerberosConfig(), topic);
    }

    @Override
    public List<List<Object>> getPreview(SourceDTO source, SqlQueryDTO queryDTO) {
        List<String> recordsFromKafka = KakfaUtil.getRecordsFromKafka(source.getUrl(), source.getBrokerUrls(), queryDTO.getTableName(), null, source.getKerberosConfig());
        List<Object> records = new ArrayList<>(recordsFromKafka);
        List<List<Object>> result = new ArrayList<>();
        result.add(records);
        return result;
    }
}
