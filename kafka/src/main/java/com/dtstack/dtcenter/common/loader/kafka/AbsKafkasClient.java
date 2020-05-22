package com.dtstack.dtcenter.common.loader.kafka;

import com.dtstack.dtcenter.loader.client.IClient;
import com.dtstack.dtcenter.loader.dto.ColumnMetaDTO;
import com.dtstack.dtcenter.loader.dto.KafkaTopicDTO;
import com.dtstack.dtcenter.loader.dto.SourceDTO;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
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
        List<String> topics = null;

        if (StringUtils.isNotBlank(source.getBrokerUrls())) {
            topics = KakfaUtil.getTopicListFromBroker(source.getBrokerUrls(), source.getKerberosConfig());
        } else {
            topics = KakfaUtil.getTopicListFromZk(source.getUrl());
        }

        //过滤特殊topic
        if (CollectionUtils.isNotEmpty(topics)) {
            topics = topics.stream().filter(s -> !"__consumer_offsets".equals(s)).collect(Collectors.toList());
        }

        return topics;
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

    /********************************* mq数据源无需实现的方法 ******************************************/
    @Override
    public Connection getCon(SourceDTO source) throws Exception {
        throw new DtLoaderException("Not Support");
    }

    @Override
    public List<Map<String, Object>> executeQuery(SourceDTO source, SqlQueryDTO queryDTO) throws Exception {
        throw new DtLoaderException("Not Support");
    }

    @Override
    public Boolean executeSqlWithoutResultSet(SourceDTO source, SqlQueryDTO queryDTO) throws Exception {
        throw new DtLoaderException("Not Support");
    }

    @Override
    public List<String> getTableList(SourceDTO source, SqlQueryDTO queryDTO) throws Exception {
        throw new DtLoaderException("Not Support");
    }

    @Override
    public List<String> getColumnClassInfo(SourceDTO source, SqlQueryDTO queryDTO) throws Exception {
        throw new DtLoaderException("Not Support");
    }

    @Override
    public List<ColumnMetaDTO> getColumnMetaData(SourceDTO source, SqlQueryDTO queryDTO) throws Exception {
        throw new DtLoaderException("Not Support");
    }

    @Override
    public List<ColumnMetaDTO> getFlinkColumnMetaData(SourceDTO source, SqlQueryDTO queryDTO) throws Exception {
        throw new DtLoaderException("Not Support");
    }

    @Override
    public String getTableMetaComment(SourceDTO source, SqlQueryDTO queryDTO) throws Exception {
        throw new DtLoaderException("Not Support");
    }
}
