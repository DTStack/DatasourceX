package com.dtstack.dtcenter.common.loader.kafkas.common;

import com.dtstack.dtcenter.common.exception.DtCenterDefException;
import com.dtstack.dtcenter.loader.dto.KafkaOffsetDTO;
import com.dtstack.dtcenter.loader.utils.TelUtil;
import com.google.common.collect.Lists;
import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.cluster.Broker;
import kafka.cluster.EndPoint;
import kafka.utils.ZkUtils;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.security.JaasUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConversions;

import java.io.File;
import java.io.IOException;
import java.util.*;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 22:46 2020/2/26
 * @Description：Kafka 工具类
 */
public class KakfaUtil {
    private static final Logger LOG = LoggerFactory.getLogger(KakfaUtil.class);

    public static boolean checkConnection(String zkUrls, String brokerUrls, Map<String, Object> kerberosConfig) {
        ZkUtils zkUtils = null;
        try {
            if (StringUtils.isEmpty(brokerUrls)) {
                brokerUrls = getAllBrokersAddressFromZk(zkUrls);
            }

            return StringUtils.isNotBlank(brokerUrls) ? checkKafkaConnection(brokerUrls, kerberosConfig) : false;
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            return false;
        } finally {
            if (zkUtils != null) {
                zkUtils.close();
            }
        }
    }

    /**
     * 从 ZK 中获取所有的 Kafka 地址
     *
     * @param zkUrls
     * @return
     * @throws Exception
     */
    public static String getAllBrokersAddressFromZk(String zkUrls) throws Exception {
        if (StringUtils.isBlank(zkUrls) || !TelUtil.checkTelnetAddr(zkUrls)) {
            throw new DtCenterDefException("请配置正确的 zookeeper 地址");
        }

        ZkUtils zkUtils = null;
        StringBuilder stringBuilder = new StringBuilder();
        try {
            zkUtils = ZkUtils.apply(zkUrls, KafkaConsistent.SESSION_TIME_OUT,
                    KafkaConsistent.CONNECTION_TIME_OUT, JaasUtils.isZkSecurityEnabled());
            List<Broker> brokers = JavaConversions.seqAsJavaList(zkUtils.getAllBrokersInCluster());
            if (CollectionUtils.isNotEmpty(brokers)) {
                for (Broker broker : brokers) {
                    List<EndPoint> endPoints = JavaConversions.seqAsJavaList(broker.endPoints());
                    for (EndPoint endPoint : endPoints) {
                        String ip = endPoint.host();
                        int port = endPoint.port();
                        if (stringBuilder.length() > 0) {
                            stringBuilder.append(",").append(ip).append(":").append(port);
                        } else {
                            stringBuilder.append(ip).append(":").append(port);
                        }
                    }
                }
            }
        } finally {
            if (zkUtils != null) {
                zkUtils.close();
            }
        }
        return stringBuilder.toString();
    }

    /**
     * 从 KAFKA 中获取 TOPIC 的信息
     *
     * @param brokerUrls
     * @param kerberosConfig
     * @return
     */
    public static List<String> getTopicListFromBroker(String brokerUrls, Map<String, Object> kerberosConfig) {
        Properties defaultKafkaConfig = initProperties(brokerUrls, kerberosConfig);
        KafkaConsumer<String, String> consumer = null;
        List<String> results = Lists.newArrayList();
        try {
            consumer = new KafkaConsumer<>(defaultKafkaConfig);
            Map<String, List<PartitionInfo>> topics = consumer.listTopics();
            if (topics != null) {
                results.addAll(topics.keySet());
            }
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        } finally {
            if (consumer != null) {
                consumer.close();
            }
        }
        return results;
    }

    /**
     * 从 ZK 中获取 TOPIC 的信息
     *
     * @param zkUrls
     * @return
     */
    public static List<String> getTopicListFromZk(String zkUrls) {
        ZkUtils zkUtils = null;
        List<String> topics = Lists.newArrayList();
        try {
            zkUtils = ZkUtils.apply(zkUrls, KafkaConsistent.SESSION_TIME_OUT,
                    KafkaConsistent.CONNECTION_TIME_OUT, JaasUtils.isZkSecurityEnabled());
            topics = JavaConversions.seqAsJavaList(zkUtils.getAllTopics());
            if (CollectionUtils.isNotEmpty(topics)) {
                topics.remove(KafkaConsistent.KAFKA_DEFAULT_CREATE_TOPIC);
            }
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        } finally {
            if (zkUtils != null) {
                zkUtils.close();
            }
        }
        return topics;
    }

    /**
     * 通过 KAFKA 中创建 TOPIC 的信息
     *
     * @param brokerUrls
     * @param kerberosConfig
     * @param topicName
     * @param partitions
     * @param replicationFacto
     * @return
     */
    public static boolean createTopicFromBroker(String brokerUrls, Map<String, Object> kerberosConfig,
                                                String topicName, Integer partitions, Integer replicationFacto) {
        // TODO 待完工
        return false;
    }

    /**
     * 通过 ZK 创建 TOPIC
     *
     * @param zkUrls
     * @param topicName
     * @param partitions
     * @param replicationFactor
     * @return
     */
    public static boolean createTopicFromZk(String zkUrls, String topicName, Integer partitions,
                                            Integer replicationFactor) {
        ZkUtils zkUtils = null;
        zkUtils = ZkUtils.apply(zkUrls, KafkaConsistent.SESSION_TIME_OUT,
                KafkaConsistent.SESSION_TIME_OUT, JaasUtils.isZkSecurityEnabled());
        try {
            partitions = null == partitions || partitions < 1 ? 1 : partitions;
            replicationFactor = null == replicationFactor || replicationFactor < 1 ? 1 : replicationFactor;
            AdminUtils.createTopic(zkUtils, topicName, partitions, replicationFactor,
                    new Properties(), RackAwareMode.Enforced$.MODULE$);
            return true;
        } catch (Exception e) {
            throw new DtCenterDefException(e.getMessage(), e);
        } finally {
            if (zkUtils != null) {
                zkUtils.close();
            }
        }
    }

    /**
     * 通过 KAFKA 获取 分区信息
     *
     * @param brokerUrls
     * @param kerberosConfig
     * @param topic
     * @return
     */
    public static List<MetadataResponse.PartitionMetadata> getAllPartitionsFromBroker(String brokerUrls,
                                                                                      Map<String, Object> kerberosConfig, String topic) {
        // TODO 待完工
        return null;
    }


    /**
     * 通过 ZK 获取 分区信息
     *
     * @param zkUrls
     * @param topic
     * @return
     */
    public static List<MetadataResponse.PartitionMetadata> getAllPartitionsFromZk(String zkUrls, String topic) {
        ZkUtils zkUtils = null;

        try {
            zkUtils = ZkUtils.apply(zkUrls, KafkaConsistent.SESSION_TIME_OUT, KafkaConsistent.CONNECTION_TIME_OUT,
                    JaasUtils.isZkSecurityEnabled());
            MetadataResponse.TopicMetadata topicMetadata = AdminUtils.fetchTopicMetadataFromZk(topic, zkUtils);
            List<MetadataResponse.PartitionMetadata> partitionMetadata = topicMetadata.partitionMetadata();
            return partitionMetadata;
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            throw new RuntimeException(e.getMessage());
        } finally {
            if (zkUtils != null) {
                zkUtils.close();
            }
        }
    }

    /**
     * 获取所有分区中最大最小的偏移量
     *
     * @param brokerUrls
     * @param kerberosConfig
     * @param topic
     * @return
     */
    public static List<KafkaOffsetDTO> getPartitionOffset(String brokerUrls,
                                                          Map<String, Object> kerberosConfig, String topic) {
        Properties defaultKafkaConfig = initProperties(brokerUrls, kerberosConfig);
        KafkaConsumer<String, String> consumer = null;
        try {
            consumer = new KafkaConsumer<>(defaultKafkaConfig);
            List<TopicPartition> partitions = new ArrayList<>();
            List<PartitionInfo> allPartitionInfo = consumer.partitionsFor(topic);
            for (PartitionInfo partitionInfo : allPartitionInfo) {
                partitions.add(new TopicPartition(partitionInfo.topic(), partitionInfo.partition()));
            }

            Map<Integer, KafkaOffsetDTO> kafkaOffsetDTOMap = new HashMap<>();
            Map<TopicPartition, Long> beginningOffsets = consumer.beginningOffsets(partitions);
            for (Map.Entry<TopicPartition, Long> entry : beginningOffsets.entrySet()) {
                KafkaOffsetDTO offsetDTO = new KafkaOffsetDTO();
                offsetDTO.setPartition(entry.getKey().partition());
                offsetDTO.setFirstOffset(entry.getValue());
                offsetDTO.setLastOffset(entry.getValue());
                kafkaOffsetDTOMap.put(entry.getKey().partition(), offsetDTO);
            }

            Map<TopicPartition, Long> endOffsets = consumer.endOffsets(partitions);
            for (Map.Entry<TopicPartition, Long> entry : endOffsets.entrySet()) {
                KafkaOffsetDTO offsetDTO = kafkaOffsetDTOMap.getOrDefault(entry.getKey().partition(),
                        new KafkaOffsetDTO());
                offsetDTO.setPartition(entry.getKey().partition());
                offsetDTO.setFirstOffset(null == offsetDTO.getFirstOffset() ? entry.getValue() :
                        offsetDTO.getFirstOffset());
                offsetDTO.setLastOffset(entry.getValue());
                kafkaOffsetDTOMap.put(entry.getKey().partition(), offsetDTO);
            }

            return (List<KafkaOffsetDTO>) kafkaOffsetDTOMap.values();
        } catch (Exception e) {
            throw new DtCenterDefException(e.getMessage(), e);
        } finally {
            if (consumer != null) {
                consumer.close();
            }
        }
    }

    /**
     * 根据 Kafka 地址 校验连接性
     *
     * @param brokerUrls
     * @param kerberosConfig
     * @return
     */
    private static boolean checkKafkaConnection(String brokerUrls, Map<String, Object> kerberosConfig) {
        boolean check = false;
        Properties props = initProperties(brokerUrls, kerberosConfig);
        /* 定义consumer */
        KafkaConsumer<String, String> consumer = null;
        try {
            consumer = new KafkaConsumer<>(props);
            consumer.listTopics();
            check = true;
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        } finally {
            if (consumer != null) {
                consumer.close();
            }
        }
        return check;
    }

    /**
     * 初始化 Kafka 配置信息
     *
     * @param brokerUrls
     * @param kerberosConfig
     * @return
     */
    private static Properties initProperties(String brokerUrls, Map<String, Object> kerberosConfig) {
        Properties props = new Properties();
        /* 定义kakfa 服务的地址，不需要将所有broker指定上 */
        props.put("bootstrap.servers", brokerUrls);
        /* 制定consumer group */
        props.put("group.id", KafkaConsistent.KAFKA_GROUP);
        /* 是否自动确认offset */
        props.put("enable.auto.commit", "true");
        /* 自动确认offset的时间间隔 */
        props.put("auto.commit.interval.ms", "1000");
        //heart beat 默认3s
        props.put("session.timeout.ms", "10000");
        //一次性的最大拉取条数
        props.put("max.poll.records", 5);
        props.put("auto.offset.reset", "earliest");
        /* key的序列化类 */
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        /* value的序列化类 */
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        if (MapUtils.isEmpty(kerberosConfig)) {
            return props;
        }

        javax.security.auth.login.Configuration.setConfiguration(null);
        String keytabConf = kerberosConfig.getOrDefault(KafkaConsistent.KAFKA_KERBEROS_KEYTAB, "").toString();
        String principal = kerberosConfig.getOrDefault(KafkaConsistent.KAFKA_KERBEROS_PRINCIPAL, "").toString();
        String kafkaKbrServiceName =
                kerberosConfig.getOrDefault(KafkaConsistent.KAFKA_KERBEROS_SERVICE_NAME, "").toString();

        if (StringUtils.isBlank(keytabConf) ||
                StringUtils.isBlank(kafkaKbrServiceName) || StringUtils.isBlank(principal)) {
            //不满足kerberos条件 直接返回
            return props;
        }
        String kafkaLoginConf = null;
        try {
            File file = new File(keytabConf);
            File jaas = new File(file.getParent() + File.separator + "kafka_jaas.conf");
            FileUtils.write(jaas, String.format(KafkaConsistent.KAFKA_JAAS_CONTENT, keytabConf, principal));
            kafkaLoginConf = jaas.getAbsolutePath();
        } catch (IOException e) {
            throw new DtCenterDefException("写入kafka配置文件异常", e);
        }
        LOG.info("Init Kafka Kerberos:login-conf:{}\n --sasl.kerberos.service.name:{}",
                keytabConf, kafkaKbrServiceName);
        // kerberos 相关设置
        props.put("security.protocol", "SASL_PLAINTEXT");
        props.put("sasl.mechanism", "GSSAPI");
        // kafka broker的启动配置
        props.put("sasl.kerberos.service.name", kafkaKbrServiceName);
        System.setProperty("java.security.auth.login.config", kafkaLoginConf);
        System.setProperty("javax.security.auth.useSubjectCredsOnly", "false");
        return props;
    }
}
