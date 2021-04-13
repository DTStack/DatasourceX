package com.dtstack.dtcenter.loader.client.sql;

import com.dtstack.dtcenter.loader.client.ClientCache;
import com.dtstack.dtcenter.loader.client.IClient;
import com.dtstack.dtcenter.loader.client.IKafka;
import com.dtstack.dtcenter.loader.dto.KafkaConsumerDTO;
import com.dtstack.dtcenter.loader.dto.KafkaOffsetDTO;
import com.dtstack.dtcenter.loader.dto.KafkaPartitionDTO;
import com.dtstack.dtcenter.loader.dto.KafkaTopicDTO;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.source.KafkaSourceDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

/**
 * @company: www.dtstack.com
 * @Author ：loader_test
 * @Date ：Created in 13:04 2020/2/29
 * @Description：Kafka 测试类
 */
@Slf4j
public class KafkaTest {

    // 构建kafka数据源信息
    private static final KafkaSourceDTO source = KafkaSourceDTO.builder()
            .url("172.16.101.236:2181,172.16.101.17:2181,172.16.100.109:2181/kafka")
            .build();

    @BeforeClass
    public static void setUp() {
        createTopic();
    }

    @Test
    public void testConForKafka() {
        IKafka client = ClientCache.getKafka(DataSourceType.KAFKA_09.getVal());
        Boolean isConnected = client.testCon(source);
        if (Boolean.FALSE.equals(isConnected)) {
            throw new DtLoaderException("connection exception");
        }
    }

    @Test
    public void testConForClient() {
        IClient client = ClientCache.getClient(DataSourceType.KAFKA_09.getVal());
        Boolean isConnected = client.testCon(source);
        if (Boolean.FALSE.equals(isConnected)) {
            throw new DtLoaderException("connection exception");
        }
    }

    @Test
    public void getAllBrokersAddress() {
        IKafka client = ClientCache.getKafka(DataSourceType.KAFKA_09.getVal());
        String brokersAddress = client.getAllBrokersAddress(source);
        assert (null != brokersAddress);
    }

    @Test
    public void getTopicList() {
        IKafka client = ClientCache.getKafka(DataSourceType.KAFKA_09.getVal());
        List<String> topicList = client.getTopicList(source);
        assert (topicList != null);
        System.out.println(topicList);
    }

    public static void createTopic() {
        try {
            IKafka client = ClientCache.getKafka(DataSourceType.KAFKA_09.getVal());
            KafkaTopicDTO topicDTO = KafkaTopicDTO.builder().partitions(3).replicationFactor((short) 1).topicName(
                    "loader_test").build();
            Boolean clientTopic = client.createTopic(source, topicDTO);
            assert (Boolean.TRUE.equals(clientTopic));
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }

    }

    @Test
    public void getOffset() {
        IKafka client = ClientCache.getKafka(DataSourceType.KAFKA_09.getVal());
        List<KafkaOffsetDTO> offset = client.getOffset(source, "loader_test");
        assert (offset != null);
    }

    @Test
    public void testPollView(){
        IKafka client = ClientCache.getKafka(DataSourceType.KAFKA_09.getVal());
        SqlQueryDTO sqlQueryDTO = SqlQueryDTO.builder().tableName("loader_test").build();
        List<List<Object>> results = client.getPreview(source,sqlQueryDTO);
        System.out.println(results);
    }

    @Test
    public void testPollViewLatest(){
        IKafka client = ClientCache.getKafka(DataSourceType.KAFKA_09.getVal());
        SqlQueryDTO sqlQueryDTO = SqlQueryDTO.builder().tableName("loader_test").build();
        List<List<Object>> results = client.getPreview(source, sqlQueryDTO, "latest");
        System.out.println(results);
    }

    @Test
    public void testTopicPartitions() {
        IKafka client = ClientCache.getKafka(DataSourceType.KAFKA_09.getVal());
        List<KafkaPartitionDTO> partitionDTOS = client.getTopicPartitions(source, "loader_test");
        Assert.assertTrue(CollectionUtils.isNotEmpty(partitionDTOS));
    }

    @Test
    public void consumeData() {
        IKafka client = ClientCache.getKafka(DataSourceType.KAFKA_09.getVal());
        List<String> result = client.consumeData(source, "loader_test", 100, "earliest", null, 60);
        Assert.assertEquals(100, result.size());
    }

    @Test
    public void consumeDataTimestamp() {
        IKafka client = ClientCache.getKafka(DataSourceType.KAFKA_09.getVal());
        // 从指定的timestamp 消费
        List<String> result = client.consumeData(source, "loader_test", 100, "timestamp", 1615871158731L, 60);
        Assert.assertEquals(100, result.size());
    }

    @Test
    public void listConsumerGroup() {
        IKafka client = ClientCache.getKafka(DataSourceType.KAFKA.getVal());
        Assert.assertTrue(CollectionUtils.isNotEmpty(client.listConsumerGroup(source)));
    }

    @Test
    public void getGroupInfoByGroupId() {
        IKafka client = ClientCache.getKafka(DataSourceType.KAFKA.getVal());
        List<KafkaConsumerDTO> info = client.getGroupInfoByGroupId(source, "test_loader222");
        Assert.assertTrue(CollectionUtils.isNotEmpty(info));
    }
}
