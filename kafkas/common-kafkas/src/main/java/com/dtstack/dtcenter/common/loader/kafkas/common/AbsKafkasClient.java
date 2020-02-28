package com.dtstack.dtcenter.common.loader.kafkas.common;

import com.dtstack.dtcenter.loader.client.IClient;
import com.dtstack.dtcenter.loader.dto.*;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.requests.MetadataResponse;

import java.sql.Connection;
import java.util.List;
import java.util.Map;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 22:39 2020/2/26
 * @Description：Kafka 数据源
 */
public abstract class AbsKafkasClient implements IClient {

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
    public List<MetadataResponse.PartitionMetadata> getAllPartitions(SourceDTO source, String topic) throws Exception {
        return KakfaUtil.getAllPartitionsFromZk(source.getUrl(), topic);
    }

    @Override
    public List<KafkaOffsetDTO> getOffset(SourceDTO source, String topic) throws Exception {
        if (StringUtils.isBlank(source.getBrokerUrls())) {
            source.setBrokerUrls(KakfaUtil.getAllBrokersAddressFromZk(source.getUrl()));
        }
        return KakfaUtil.getPartitionOffset(source.getBrokerUrls(), source.getKerberosConfig(), topic);
    }

    /********************************* Kafka 数据库无需实现的方法 ******************************************/
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
}
