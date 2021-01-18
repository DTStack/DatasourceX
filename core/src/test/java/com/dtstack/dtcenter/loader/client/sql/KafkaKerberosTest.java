package com.dtstack.dtcenter.loader.client.sql;

import com.dtstack.dtcenter.loader.client.ClientCache;
import com.dtstack.dtcenter.loader.client.IClient;
import com.dtstack.dtcenter.loader.client.IKafka;
import com.dtstack.dtcenter.loader.client.IKerberos;
import com.dtstack.dtcenter.loader.dto.KafkaOffsetDTO;
import com.dtstack.dtcenter.loader.dto.KafkaTopicDTO;
import com.dtstack.dtcenter.loader.dto.source.KafkaSourceDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.dtstack.dtcenter.loader.kerberos.HadoopConfTool;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.requests.MetadataResponse;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 11:27 2020/9/10
 * @Description：Kafka Kerberos 认证
 */
@Slf4j
@Ignore
public class KafkaKerberosTest {
    KafkaSourceDTO source = KafkaSourceDTO.builder()
            .url("172.16.100.170:2181,172.16.100.191:2181,172.16.101.123:2181")
            .brokerUrls("172.16.100.170:9092,172.16.100.191:9092,172.16.101.123:9092")
            .build();

    @Before
    public void setUp() throws Exception {
        // 准备 Kerberos 参数
        Map<String, Object> kerberosConfig = new HashMap<>();
        kerberosConfig.put(HadoopConfTool.PRINCIPAL_FILE, "/kafka-cdh1.keytab");
        kerberosConfig.put(HadoopConfTool.KEY_JAVA_SECURITY_KRB5_CONF, "/krb5.conf");
        source.setKerberosConfig(kerberosConfig);

        String localKerberosPath = KafkaKerberosTest.class.getResource("/eng-cdh").getPath();
        IKerberos kerberos = ClientCache.getKerberos(DataSourceType.KAFKA.getVal());
        kerberos.prepareKerberosForConnect(kerberosConfig, localKerberosPath);

        createTopic();
    }

    @Test
    public void testConForKafka() throws Exception {
        IKafka client = ClientCache.getKafka(DataSourceType.KAFKA_09.getVal());
        Boolean isConnected = client.testCon(source);
        if (Boolean.FALSE.equals(isConnected)) {
            throw new DtLoaderException("连接异常");
        }
    }

    @Test
    public void testConForClient() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.KAFKA_09.getVal());
        Boolean isConnected = client.testCon(source);
        if (Boolean.FALSE.equals(isConnected)) {
            throw new DtLoaderException("连接异常");
        }
    }

    @Test
    public void getAllBrokersAddress() throws Exception {
        IKafka client = ClientCache.getKafka(DataSourceType.KAFKA_09.getVal());
        String brokersAddress = client.getAllBrokersAddress(source);
        assert (null != brokersAddress);
    }

    @Test
    public void getTopicList() throws Exception {
        IKafka client = ClientCache.getKafka(DataSourceType.KAFKA_09.getVal());
        List<String> topicList = client.getTopicList(source);
        assert (topicList != null);
        System.out.println(topicList);
    }

    @Test
    public void createTopic() throws Exception {
        IKafka client = ClientCache.getKafka(DataSourceType.KAFKA_09.getVal());
        KafkaTopicDTO topicDTO = KafkaTopicDTO.builder().partitions(1).replicationFactor((short) 1).topicName(
                "nanqi").build();
        Boolean clientTopic = client.createTopic(source, topicDTO);
        assert (Boolean.TRUE.equals(clientTopic));
    }

    @Test
    public void getAllPartitions() throws Exception {
        // 测试的时候需要引进 kafka 包
        IKafka client = ClientCache.getKafka(DataSourceType.KAFKA_09.getVal());
        List<MetadataResponse.PartitionMetadata> allPartitions = client.getAllPartitions(source, "nanqi");
        System.out.println(allPartitions.size());
    }

    @Test
    public void getOffset() throws Exception {
        IKafka client = ClientCache.getKafka(DataSourceType.KAFKA_09.getVal());
        List<KafkaOffsetDTO> offset = client.getOffset(source, "nanqi");
        assert (offset != null);
    }
}
