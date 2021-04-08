package com.dtstack.dtcenter.common.loader.kafka.util;

import com.dtstack.dtcenter.common.loader.common.exception.IErrorPattern;
import com.dtstack.dtcenter.common.loader.common.service.ErrorAdapterImpl;
import com.dtstack.dtcenter.common.loader.common.service.IErrorAdapter;
import com.dtstack.dtcenter.common.loader.common.utils.TelUtil;
import com.dtstack.dtcenter.common.loader.kafka.KafkaConsistent;
import com.dtstack.dtcenter.common.loader.kafka.KafkaErrorPattern;
import com.dtstack.dtcenter.common.loader.kafka.enums.EConsumeType;
import com.dtstack.dtcenter.loader.dto.KafkaOffsetDTO;
import com.dtstack.dtcenter.loader.dto.KafkaPartitionDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.dtstack.dtcenter.loader.kerberos.HadoopConfTool;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import kafka.admin.AdminUtils;
import kafka.cluster.Broker;
import kafka.cluster.EndPoint;
import kafka.utils.ZkUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.security.JaasUtils;
import scala.collection.JavaConversions;
import sun.security.krb5.Config;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 22:46 2020/2/26
 * @Description：Kafka 工具类
 */
@Slf4j
public class KafkaUtil {

    public static final String EARLIEST = "earliest";
    private static final int MAX_POOL_RECORDS = 5;

    private static final IErrorPattern ERROR_PATTERN = new KafkaErrorPattern();

    // 异常适配器
    private static final IErrorAdapter ERROR_ADAPTER = new ErrorAdapterImpl();


    public static boolean checkConnection(String zkUrls, String brokerUrls, Map<String, Object> kerberosConfig) {
        ZkUtils zkUtils = null;
        try {
            if (StringUtils.isEmpty(brokerUrls)) {
                brokerUrls = getAllBrokersAddressFromZk(zkUrls);
            }
            return StringUtils.isNotBlank(brokerUrls) ? checkKafkaConnection(brokerUrls, kerberosConfig) : false;
        } catch (Exception e) {
            throw new DtLoaderException(ERROR_ADAPTER.connAdapter(e.getMessage(), ERROR_PATTERN), e);
        } finally {
            if (zkUtils != null) {
                zkUtils.close();
            }
        }
    }

    /**
     * 写kafka jaas文件，同时处理 krb5.conf
     * @param kerberosConfig
     * @return jaas文件绝对路径
     */
    private static String writeKafkaJaas(Map<String, Object> kerberosConfig) {
        log.info("Initialize Kafka JAAS file, kerberosConfig : {}", kerberosConfig);
        if (MapUtils.isEmpty(kerberosConfig)){
            return null;
        }

        // 处理 krb5.conf
        if (kerberosConfig.containsKey(HadoopConfTool.KEY_JAVA_SECURITY_KRB5_CONF)) {
            System.setProperty(HadoopConfTool.KEY_JAVA_SECURITY_KRB5_CONF, MapUtils.getString(kerberosConfig, HadoopConfTool.KEY_JAVA_SECURITY_KRB5_CONF));
        }

        String keytabConf = MapUtils.getString(kerberosConfig, HadoopConfTool.PRINCIPAL_FILE);
        // 兼容历史数据
        keytabConf = StringUtils.isBlank(keytabConf) ? MapUtils.getString(kerberosConfig, HadoopConfTool.KAFKA_KERBEROS_KEYTAB) : keytabConf;
        try {
            File file = new File(keytabConf);
            File jaas = new File(file.getParent() + File.separator + "kafka_jaas.conf");
            if (jaas.exists()) {
                jaas.delete();
            }

            String principal = MapUtils.getString(kerberosConfig, HadoopConfTool.PRINCIPAL);
            // 历史数据兼容
            principal = StringUtils.isBlank(principal) ? MapUtils.getString(kerberosConfig, "kafka.kerberos.principal") : principal;
            FileUtils.write(jaas, String.format(KafkaConsistent.KAFKA_JAAS_CONTENT, keytabConf, principal));
            String kafkaLoginConf = jaas.getAbsolutePath();
            log.info("Init Kafka Kerberos:login-conf:{}\n --sasl.kerberos.service.name:{}", keytabConf, principal);
            return kafkaLoginConf;
        } catch (IOException e) {
            throw new DtLoaderException("Writing to Kafka configuration file exception", e);
        }
    }

    /**
     * 从 ZK 中获取所有的 Kafka 地址
     *
     * @param zkUrls
     * @return
     * @throws Exception
     */
    public static String getAllBrokersAddressFromZk(String zkUrls) {
        log.info("Obtain Kafka Broker address through ZK : {}", zkUrls);
        if (StringUtils.isBlank(zkUrls) || !TelUtil.checkTelnetAddr(zkUrls)) {
            throw new DtLoaderException("Please configure the correct zookeeper address");
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
        List<String> results = Lists.newArrayList();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(defaultKafkaConfig)) {
            ;
            Map<String, List<PartitionInfo>> topics = consumer.listTopics();
            if (topics != null) {
                results.addAll(topics.keySet());
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        } finally {
            destroyProperty();
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
        log.info("Get Kafka Topic information through ZK: {}", zkUrls);
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
            log.error(e.getMessage(), e);
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
    public static void createTopicFromBroker(String brokerUrls, Map<String, Object> kerberosConfig,
                                                String topicName, Integer partitions, Short replicationFacto) {
        Properties defaultKafkaConfig = initProperties(brokerUrls, kerberosConfig);
        try (AdminClient client = AdminClient.create(defaultKafkaConfig);) {
            NewTopic topic = new NewTopic(topicName, partitions, replicationFacto);
            client.createTopics(Collections.singleton(topic));
        } catch (Exception e) {
            throw new DtLoaderException(e.getMessage(), e);
        }
    }

    /**
     * 通过 ZK 获取 分区信息 (目前没有地方使用到)
     *
     * @param zkUrls
     * @param topic
     * @return
     */
    @Deprecated
    public static List<MetadataResponse.PartitionMetadata> getAllPartitionsFromZk(String zkUrls, String topic) {
        log.info("Obtain Kafka partition information through ZK, zkUrls : {}, topic : {}", zkUrls, topic);
        ZkUtils zkUtils = null;

        try {
            zkUtils = ZkUtils.apply(zkUrls, KafkaConsistent.SESSION_TIME_OUT, KafkaConsistent.CONNECTION_TIME_OUT,
                    JaasUtils.isZkSecurityEnabled());
            MetadataResponse.TopicMetadata topicMetadata = AdminUtils.fetchTopicMetadataFromZk(topic, zkUtils);
            List<MetadataResponse.PartitionMetadata> partitionMetadata = topicMetadata.partitionMetadata();
            return partitionMetadata;
        } catch (Exception e) {
            throw new DtLoaderException(e.getMessage());
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
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(defaultKafkaConfig)) {
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

            return kafkaOffsetDTOMap.values().stream().collect(Collectors.toList());
        } catch (Exception e) {
            throw new DtLoaderException(e.getMessage(), e);
        } finally {
            destroyProperty();
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
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.listTopics();
            check = true;
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        } finally {
            destroyProperty();
        }
        return check;
    }

    /**
     * 初始化 Kafka 配置信息
     *
     */
    public static Properties initProperties (String zkUrls, String brokerUrls,Map<String, Object> conf) {
        String bootstrapServers;
        if (StringUtils.isEmpty(brokerUrls)){
            bootstrapServers = getAllBrokersAddressFromZk(zkUrls);
        }else {
            bootstrapServers = brokerUrls;
        }
        return initProperties(bootstrapServers, conf);
    }

    private static void destroyProperty() {
        System.clearProperty("java.security.auth.login.config");
        System.clearProperty("javax.security.auth.useSubjectCredsOnly");
    }

    /**
     * 初始化 Kafka 配置信息
     *
     * @param brokerUrls
     * @param kerberosConfig
     * @return
     */
    private synchronized static Properties initProperties(String brokerUrls, Map<String, Object> kerberosConfig) {
        log.info("Initialize Kafka configuration information, brokerUrls : {}, kerberosConfig : {}", brokerUrls, kerberosConfig);
        Properties props = new Properties();
        if (StringUtils.isBlank(brokerUrls)) {
            throw new DtLoaderException("Kafka Broker address cannot be empty");
        }
        /* 定义kakfa 服务的地址，不需要将所有broker指定上 */
        props.put("bootstrap.servers", brokerUrls);
        /* 是否自动确认offset */
        props.put("enable.auto.commit", "true");
        /* 设置group id */
        props.put("group.id", KafkaConsistent.KAFKA_GROUP);
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

        /*设置超时时间*/
        props.put("request.timeout.ms", "10500");
        if (MapUtils.isEmpty(kerberosConfig)) {
            return props;
        }

        // 历史数据兼容
        if (MapUtils.isEmpty(kerberosConfig)) {
            //不满足kerberos条件 直接返回
            return props;
        }

        // 只需要认证的用户名
        String kafkaKbrServiceName = MapUtils.getString(kerberosConfig, HadoopConfTool.KAFKA_KERBEROS_SERVICE_NAME);
        kafkaKbrServiceName = kafkaKbrServiceName.split("/")[0];
        String kafkaLoginConf = writeKafkaJaas(kerberosConfig);

        // 刷新kerberos认证信息，在设置完java.security.krb5.conf后进行，否则会使用上次的krb5文件进行 refresh 导致认证失败
        try {
            Config.refresh();
            javax.security.auth.login.Configuration.setConfiguration(null);
        } catch (Exception e) {
            log.error("Kafka kerberos authentication information refresh failed!");
        }
        // kerberos 相关设置
        props.put("security.protocol", "SASL_PLAINTEXT");
        props.put("sasl.mechanism", "GSSAPI");
        // kafka broker的启动配置
        props.put("sasl.kerberos.service.name", kafkaKbrServiceName);
        System.setProperty("java.security.auth.login.config", kafkaLoginConf);
        System.setProperty("javax.security.auth.useSubjectCredsOnly", "false");
        return props;
    }


    public static List<String> getRecordsFromKafka(String zkUrls, String brokerUrls, String topic, String autoReset, Map<String, Object> kerberos) {
        List<String> result = new ArrayList<>();

        Properties props = initProperties(zkUrls, brokerUrls, kerberos);
        /*去除超时时间*/
        props.remove("request.timeout.ms");
        props.put("max.poll.records", MAX_POOL_RECORDS);
        /* 定义consumer */
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);) {
            List<TopicPartition> partitions = new ArrayList<>();
            List<PartitionInfo> all = consumer.partitionsFor(topic);
            for (PartitionInfo partitionInfo : all) {
                partitions.add(new TopicPartition(partitionInfo.topic(), partitionInfo.partition()));
            }

            consumer.assign(partitions);
            //如果消息没有被消费过，可能出现无法移动offset的情况导致报错
            //https://stackoverflow.com/questions/41008610/kafkaconsumer-0-10-java-api-error-message-no-current-assignment-for-partition
            //主动拉去一次消息
            consumer.poll(1000);

            //根据autoReset 设置位移
            if (EARLIEST.equals(autoReset)) {
                consumer.seekToBeginning(partitions);
            } else {
                Map<TopicPartition, Long> partitionLongMap = consumer.endOffsets(partitions);
                for (Map.Entry<TopicPartition, Long> entry : partitionLongMap.entrySet()) {
                    long offset = entry.getValue() - MAX_POOL_RECORDS;
                    offset = offset > 0 ? offset : 0;
                    consumer.seek(entry.getKey(), offset);
                }
            }

            /* 读取数据，读取超时时间为100ms */
            ConsumerRecords<String, String> records = consumer.poll(1000);
            for (ConsumerRecord<String, String> record : records) {
                String value = record.value();
                if (StringUtils.isBlank(value)) {
                    continue;
                }
                if (result.size() >= MAX_POOL_RECORDS) {
                    break;
                }
                result.add(record.value());
            }
        } catch (Exception e) {
            log.error("consumption data from Kafka zkUrls:{} \nbrokerUrls:{} \ntopic:{} \nautoReset:{} \n ", zkUrls, brokerUrls, topic, autoReset, e);
        } finally {
            destroyProperty();
        }
        return result;
    }

    public static List<KafkaPartitionDTO> getPartitions (String brokerUrls, String topic, Map<String, Object> kerberosConfig) {
        Properties defaultKafkaConfig = initProperties(brokerUrls, kerberosConfig);
        List<KafkaPartitionDTO> partitionDTOS = Lists.newArrayList();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(defaultKafkaConfig)) {
            // PartitionInfo没有实现序列化接口，不能使用 fastJson 进行拷贝
            List<PartitionInfo> partitions = consumer.partitionsFor(topic);
            if (CollectionUtils.isEmpty(partitions)) {
                return partitionDTOS;
            }
            for (PartitionInfo partition : partitions) {
                // 所有副本
                List<KafkaPartitionDTO.Node> replicas = Lists.newArrayList();
                for (Node node : partition.replicas()) {
                    replicas.add(buildKafkaPartitionNode(node));
                }
                // 在isr队列中的副本
                List<KafkaPartitionDTO.Node> inSyncReplicas = Lists.newArrayList();
                for (Node node : partition.inSyncReplicas()) {
                    inSyncReplicas.add(buildKafkaPartitionNode(node));
                }
                KafkaPartitionDTO kafkaPartitionDTO = KafkaPartitionDTO.builder()
                        .topic(partition.topic())
                        .partition(partition.partition())
                        .leader(buildKafkaPartitionNode(partition.leader()))
                        .replicas(replicas.toArray(new KafkaPartitionDTO.Node[]{}))
                        .inSyncReplicas(inSyncReplicas.toArray(new KafkaPartitionDTO.Node[]{}))
                        .build();
                partitionDTOS.add(kafkaPartitionDTO);
            }
            return partitionDTOS;
        } catch (Exception e) {
            throw new DtLoaderException(String.format("Get topic: %s partition information is exception：%s", topic, e.getMessage()), e);
        }
    }

    /**
     * 构建kafka node
     * @param node kafka副本信息
     * @return common-loader中定义的kafka副本信息
     */
    private static KafkaPartitionDTO.Node buildKafkaPartitionNode(Node node) {
        if (Objects.isNull(node)) {
            return KafkaPartitionDTO.Node.builder().build();
        }
        return KafkaPartitionDTO.Node.builder()
                .host(node.host())
                .id(node.id())
                .idString(node.idString())
                .port(node.port())
                .rack(node.rack())
                .build();
    }


    /**
     * 从 kafka 消费数据
     *
     * @param brokerUrls      kafka broker节点信息
     * @param topic           消费主题
     * @param collectNum      收集条数
     * @param offsetReset     消费方式
     * @param timestampOffset 按时间消费
     * @param maxTimeWait     最大等待时间
     * @param kerberosConfig  kerberos 配置
     * @return 消费到的数据
     */
    public static List<String> consumeData(String brokerUrls, String topic, Integer collectNum,
                                           String offsetReset, Long timestampOffset,
                                           Integer maxTimeWait, Map<String, Object> kerberosConfig) {
        // 结果集
        List<String> result = new ArrayList<>();
        Properties prop = initProperties(brokerUrls, kerberosConfig);
        // 每次拉取最大条数
        prop.put("max.poll.records", MAX_POOL_RECORDS);
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(prop)) {
            List<TopicPartition> partitions = Lists.newArrayList();
            // 获取所有的分区
            List<PartitionInfo> allPartitions = consumer.partitionsFor(topic);
            for (PartitionInfo partitionInfo : allPartitions) {
                partitions.add(new TopicPartition(partitionInfo.topic(), partitionInfo.partition()));
            }
            consumer.assign(partitions);

            // 从最早位置开始消费
            if (EConsumeType.EARLIEST.name().toLowerCase().equals(offsetReset)) {
                consumer.seekToBeginning(partitions);
            } else if (EConsumeType.TIMESTAMP.name().toLowerCase().equals(offsetReset) && Objects.nonNull(timestampOffset)) {
                Map<TopicPartition, Long> timestampsToSearch = Maps.newHashMap();
                for (TopicPartition partition : partitions) {
                    timestampsToSearch.put(partition, timestampOffset);
                }
                Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes = consumer.offsetsForTimes(timestampsToSearch);
                // 没有找到offset 则从当前时间开始消费
                if (MapUtils.isEmpty(offsetsForTimes)) {
                    consumer.seekToEnd(partitions);
                } else {
                    for (Map.Entry<TopicPartition, OffsetAndTimestamp> entry : offsetsForTimes.entrySet()) {
                        consumer.seek(entry.getKey(), entry.getValue().offset());
                    }
                }
            } else {
                // 默认从最当前位置开始消费
                if (EConsumeType.LATEST.name().toLowerCase().equals(offsetReset)) {
                    consumer.seekToEnd(partitions);
                }
            }

            // 开始时间
            long start = System.currentTimeMillis();
            // 消费结束时间
            long endTime = start + maxTimeWait * 1000;
            while (true) {
                long nowTime = System.currentTimeMillis();
                if (nowTime >= endTime) {
                    break;
                }
                ConsumerRecords<String, String> records = consumer.poll(1000);
                for (ConsumerRecord<String, String> record : records) {
                    String value = record.value();
                    if (StringUtils.isBlank(value)) {
                        continue;
                    }
                    result.add(value);
                    if (result.size() >= collectNum) {
                        break;
                    }
                }
                if (result.size() >= collectNum) {
                    break;
                }
            }
        } catch (Exception e) {
            log.error("consumption data from Kafka exception：brokerUrls:{} \ntopic:{} \noffsetReset:{} \n timestampOffset:{} \n maxTimeWait:{} \n ", brokerUrls, topic, offsetReset, timestampOffset, maxTimeWait, e);
        } finally {
            destroyProperty();
        }
        return result;
    }
}
