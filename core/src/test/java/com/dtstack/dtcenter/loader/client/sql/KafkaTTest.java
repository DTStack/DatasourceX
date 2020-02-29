package com.dtstack.dtcenter.loader.client.sql;

import com.dtstack.dtcenter.common.enums.DataSourceType;
import com.dtstack.dtcenter.common.exception.DtCenterDefException;
import com.dtstack.dtcenter.loader.client.AbsClientCache;
import com.dtstack.dtcenter.loader.client.IClient;
import com.dtstack.dtcenter.loader.dto.KafkaOffsetDTO;
import com.dtstack.dtcenter.loader.dto.KafkaTopicDTO;
import com.dtstack.dtcenter.loader.dto.SourceDTO;
import com.dtstack.dtcenter.loader.enums.ClientType;
import org.apache.kafka.common.requests.MetadataResponse;
import org.junit.Test;

import java.util.List;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 13:04 2020/2/29
 * @Description：Kafka 测试类
 */
public class KafkaTTest {
    private static final AbsClientCache clientCache = ClientType.DATA_SOURCE_CLIENT.getClientCache();

    SourceDTO source = SourceDTO.builder()
            .url("kudu1:2181/kafka")
            .build();

    @Test
    public void testCon() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.KAFKA_09.getVal());
        Boolean isConnected = client.testCon(source);
        if (Boolean.FALSE.equals(isConnected)) {
            throw new DtCenterDefException("连接异常");
        }
    }

    @Test
    public void getAllBrokersAddress() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.KAFKA_09.getVal());
        String brokersAddress = client.getAllBrokersAddress(source);
        System.out.println(brokersAddress);
    }

    @Test
    public void getTopicList() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.KAFKA_09.getVal());
        List<String> topicList = client.getTopicList(source);
        System.out.println(topicList.size());
    }

    @Test
    public void createTopic() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.KAFKA_09.getVal());
        KafkaTopicDTO topicDTO = KafkaTopicDTO.builder().partitions(1).replicationFactor(1).topicName(
                "nanqi_200229").build();
        Boolean clientTopic = client.createTopic(source, topicDTO);
        System.out.println(clientTopic);
    }

    @Test
    public void getAllPartitions() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.KAFKA_09.getVal());
        List<MetadataResponse.PartitionMetadata> allPartitions = client.getAllPartitions(source, "es_teset");
        System.out.println(allPartitions.size());
    }

    @Test
    public void getOffset() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.KAFKA_09.getVal());
        List<KafkaOffsetDTO> offset = client.getOffset(source, "es_teset");
        System.out.println(offset.size());
    }
}
