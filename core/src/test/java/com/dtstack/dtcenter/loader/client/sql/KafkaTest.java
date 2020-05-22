package com.dtstack.dtcenter.loader.client.sql;

import com.dtstack.dtcenter.common.enums.DataSourceClientType;
import com.dtstack.dtcenter.common.exception.DtCenterDefException;
import com.dtstack.dtcenter.loader.client.AbsClientCache;
import com.dtstack.dtcenter.loader.client.IClient;
import com.dtstack.dtcenter.loader.dto.KafkaOffsetDTO;
import com.dtstack.dtcenter.loader.dto.KafkaTopicDTO;
import com.dtstack.dtcenter.loader.dto.source.KafkaSourceDTO;
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
public class KafkaTest {
    private static final AbsClientCache clientCache = ClientType.DATA_SOURCE_CLIENT.getClientCache();

    KafkaSourceDTO source = KafkaSourceDTO.builder()
            .url("kudu3:2181/kafka")
            .build();

    @Test
    public void testCon() throws Exception {
        IClient client = clientCache.getClient(DataSourceClientType.KAFKA_09.getPluginName());
        Boolean isConnected = client.testCon(source);
        if (Boolean.FALSE.equals(isConnected)) {
            throw new DtCenterDefException("连接异常");
        }
    }

    @Test
    public void getAllBrokersAddress() throws Exception {
        IClient client = clientCache.getClient(DataSourceClientType.KAFKA_09.getPluginName());
        String brokersAddress = client.getAllBrokersAddress(source);
        assert (null != brokersAddress);
    }

    @Test
    public void getTopicList() throws Exception {
        IClient client = clientCache.getClient(DataSourceClientType.KAFKA_09.getPluginName());
        List<String> topicList = client.getTopicList(source);
        assert (topicList != null);
    }

    @Test
    public void createTopic() throws Exception {
        IClient client = clientCache.getClient(DataSourceClientType.KAFKA_09.getPluginName());
        KafkaTopicDTO topicDTO = KafkaTopicDTO.builder().partitions(1).replicationFactor(1).topicName(
                "nanqi02").build();
        Boolean clientTopic = client.createTopic(source, topicDTO);
        assert (Boolean.TRUE.equals(clientTopic));
    }

    @Test
    public void getAllPartitions() throws Exception {
        // 测试的时候需要引进 kafka 包
        IClient client = clientCache.getClient(DataSourceClientType.KAFKA_09.getPluginName());
        List<MetadataResponse.PartitionMetadata> allPartitions = client.getAllPartitions(source, "nanqi01");
        System.out.println(allPartitions.size());
    }

    @Test
    public void getOffset() throws Exception {
        IClient client = clientCache.getClient(DataSourceClientType.KAFKA_09.getPluginName());
        List<KafkaOffsetDTO> offset = client.getOffset(source, "nanqi01");
        assert (offset != null);
    }
}
