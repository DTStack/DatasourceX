package com.dtstack.dtcenter.loader.client.sql;

import com.dtstack.dtcenter.loader.client.ClientCache;
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
import org.junit.BeforeClass;
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
public class KafkaKerberosWithoutBrokerUrlsTest {
    
    // 构建数据源信息
    private static final KafkaSourceDTO source = KafkaSourceDTO.builder()
            //.url("172.16.101.159:2181/kafka")
            .brokerUrls("172.16.101.159:9092")
            .build();

    @BeforeClass
    public static void setUp(){
        // 准备 Kerberos 参数
        Map<String, Object> kerberosConfig = new HashMap<>();
        kerberosConfig.put(HadoopConfTool.KAFKA_KERBEROS_SERVICE_NAME, "kafka");
        kerberosConfig.put(HadoopConfTool.PRINCIPAL, "kafka/krbt1@DTSACK.COM");
        kerberosConfig.put(HadoopConfTool.PRINCIPAL_FILE, "/kafka.keytab");
        kerberosConfig.put(HadoopConfTool.KEY_JAVA_SECURITY_KRB5_CONF, "/krb5_2.conf");
        source.setKerberosConfig(kerberosConfig);

        String localKerberosPath = KafkaKerberosWithoutBrokerUrlsTest.class.getResource("/krbt").getPath();
        IKerberos kerberos = ClientCache.getKerberos(DataSourceType.KAFKA.getVal());
        kerberos.prepareKerberosForConnect(kerberosConfig, localKerberosPath);

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
    public void getAllBrokersAddress(){
        IKafka client = ClientCache.getKafka(DataSourceType.KAFKA_09.getVal());
        String brokersAddress = client.getAllBrokersAddress(source);
        assert (null != brokersAddress);
    }

    @Test
    public void getTopicList(){
        IKafka client = ClientCache.getKafka(DataSourceType.KAFKA_09.getVal());
        List<String> topicList = client.getTopicList(source);
        assert (topicList != null);
        System.out.println(topicList);
    }
    
    public static void createTopic(){
        IKafka client = ClientCache.getKafka(DataSourceType.KAFKA_09.getVal());
        KafkaTopicDTO topicDTO = KafkaTopicDTO.builder().partitions(1).replicationFactor((short) 1).topicName(
                "nanqi").build();
        Boolean clientTopic = client.createTopic(source, topicDTO);
        assert (Boolean.TRUE.equals(clientTopic));
    }

    @Test
    public void getAllPartitions(){
        // 测试的时候需要引进 kafka 包
        IKafka client = ClientCache.getKafka(DataSourceType.KAFKA_09.getVal());
        List<MetadataResponse.PartitionMetadata> allPartitions = client.getAllPartitions(source, "nanqi");
        System.out.println(allPartitions.size());
    }

    @Test
    public void getOffset(){
        IKafka client = ClientCache.getKafka(DataSourceType.KAFKA_09.getVal());
        List<KafkaOffsetDTO> offset = client.getOffset(source, "nanqi");
        assert (offset != null);
    }
}
