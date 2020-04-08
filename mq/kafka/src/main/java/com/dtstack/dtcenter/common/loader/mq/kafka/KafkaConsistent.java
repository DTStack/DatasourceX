package com.dtstack.dtcenter.common.loader.mq.kafka;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 22:56 2020/2/26
 * @Description：TODO
 */
public class KafkaConsistent {
    /**
     * Kafka 会话超时时间
     */
    public static final int SESSION_TIME_OUT = 30000;

    /**
     * Kafka 连接超时时间
     */
    public static final int CONNECTION_TIME_OUT = 5000;

    /**
     * Kafka 默认创建的 TOPIC 名称
     */
    public static final String KAFKA_DEFAULT_CREATE_TOPIC = "__consumer_offsets";

    /**
     * Kafka 默认组名称
     */
    public static final String KAFKA_GROUP = "STREAM_APP_KAFKA";

    /**
     * Kafka JAAS 内容
     */
    public static String KAFKA_JAAS_CONTENT = "KafkaClient {\n" +
            "    com.sun.security.auth.module.Krb5LoginModule required\n" +
            "    useKeyTab=true\n" +
            "    storeKey=true\n" +
            "    keyTab=\"%s\"\n" +
            "    principal=\"%s\";\n" +
            "};";

    /**
     * KAFKA Kerberos 相关配置
     */
    public static final String KAFKA_KERBEROS_KEYTAB = "kafka.kerberos.keytab";
    public static final String KAFKA_KERBEROS_PRINCIPAL = "kafka.kerberos.principal";
    public static final String KAFKA_KERBEROS_SERVICE_NAME = "sasl.kerberos.service.name";
}
