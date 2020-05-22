package com.dtstack.dtcenter.common.loader.kafka;

import com.dtstack.dtcenter.loader.client.IClient;
import com.dtstack.dtcenter.loader.dto.ColumnMetaDTO;
import com.dtstack.dtcenter.loader.dto.KafkaTopicDTO;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;
import com.dtstack.dtcenter.loader.dto.source.KafkaSourceDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 22:39 2020/2/26
 * @Description：Kafka 数据源
 */
public abstract class AbsKafkasClient<T> implements IClient<T> {

    @Override
    public Boolean testCon(ISourceDTO iSource) {
        KafkaSourceDTO kafkaSourceDTO = (KafkaSourceDTO) iSource;
        return KakfaUtil.checkConnection(kafkaSourceDTO.getUrl(), kafkaSourceDTO.getBrokerUrls(),
                kafkaSourceDTO.getKerberosConfig());
    }

    @Override
    public String getAllBrokersAddress(ISourceDTO iSource) throws Exception {
        KafkaSourceDTO kafkaSourceDTO = (KafkaSourceDTO) iSource;
        if (StringUtils.isNotBlank(kafkaSourceDTO.getBrokerUrls())) {
            return kafkaSourceDTO.getBrokerUrls();
        }
        return KakfaUtil.getAllBrokersAddressFromZk(kafkaSourceDTO.getUrl());
    }

    @Override
    public List<String> getTopicList(ISourceDTO iSource) throws Exception {
        List<String> topics = null;
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
    public Boolean createTopic(ISourceDTO iSource, KafkaTopicDTO kafkaTopic) throws Exception {
        KafkaSourceDTO kafkaSourceDTO = (KafkaSourceDTO) iSource;
        return KakfaUtil.createTopicFromZk(kafkaSourceDTO.getUrl(), kafkaTopic.getTopicName(),
                kafkaTopic.getPartitions(),
                kafkaTopic.getReplicationFactor());
    }

    @Override
    public List<T> getAllPartitions(ISourceDTO iSource, String topic) throws Exception {
        KafkaSourceDTO kafkaSourceDTO = (KafkaSourceDTO) iSource;
        return (List<T>) KakfaUtil.getAllPartitionsFromZk(kafkaSourceDTO.getUrl(), topic);
    }

    @Override
    public List getOffset(ISourceDTO iSource, String topic) throws Exception {
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

    /********************************* mq数据源无需实现的方法 ******************************************/
    @Override
    public Connection getCon(ISourceDTO iSource) throws Exception {
        throw new DtLoaderException("Not Support");
    }

    @Override
    public List<Map<String, Object>> executeQuery(ISourceDTO iSource, SqlQueryDTO queryDTO) throws Exception {
        throw new DtLoaderException("Not Support");
    }

    @Override
    public Boolean executeSqlWithoutResultSet(ISourceDTO iSource, SqlQueryDTO queryDTO) throws Exception {
        throw new DtLoaderException("Not Support");
    }

    @Override
    public List<String> getTableList(ISourceDTO iSource, SqlQueryDTO queryDTO) throws Exception {
        throw new DtLoaderException("Not Support");
    }

    @Override
    public List<String> getColumnClassInfo(ISourceDTO iSource, SqlQueryDTO queryDTO) throws Exception {
        throw new DtLoaderException("Not Support");
    }

    @Override
    public List<ColumnMetaDTO> getColumnMetaData(ISourceDTO iSource, SqlQueryDTO queryDTO) throws Exception {
        throw new DtLoaderException("Not Support");
    }

    @Override
    public List<ColumnMetaDTO> getFlinkColumnMetaData(ISourceDTO iSource, SqlQueryDTO queryDTO) throws Exception {
        throw new DtLoaderException("Not Support");
    }

    @Override
    public String getTableMetaComment(ISourceDTO iSource, SqlQueryDTO queryDTO) throws Exception {
        throw new DtLoaderException("Not Support");
    }
}
