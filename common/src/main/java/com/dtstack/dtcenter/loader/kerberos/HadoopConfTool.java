package com.dtstack.dtcenter.loader.kerberos;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 20:57 2020/8/27
 * @Description：Hadoop 配置中心 11 月份删除与 getPrincipal 一起
 */
@Deprecated
public class HadoopConfTool {
    /**
     * Hadoop 开启 Kerberos 是否需要二次认证
     */
    public static final String HADOOP_SECURITY_AUTHORIZATION = "hadoop.security.authorization";

    /**
     * krb5 系统属性键
     */
    public static final String KEY_JAVA_SECURITY_KRB5_CONF = "java.security.krb5.conf";

    /**
     * principal 键
     */
    public static final String PRINCIPAL = "principal";

    /**
     * Hbase master Principal 键
     */
    public static final String HBASE_MASTER_PRINCIPAL = "hbase.master.kerberos.principal";

    /**
     * Hbase region Principal 键
     */
    public static final String HBASE_REGION_PRINCIPAL = "hbase.regionserver.kerberos.principal";

    /**
     * principal 文件 键
     */
    public static final String PRINCIPAL_FILE = "principalFile";

    /**
     * Kafka kerberos keytab 键
     */
    public static final String KAFKA_KERBEROS_KEYTAB = "kafka.kerberos.keytab";

    /**
     * Kafka Principal 参数，也可选 Principal
     */
    public static final String KAFKA_KERBEROS_SERVICE_NAME = "sasl.kerberos.service.name";

    /**
     * Resource Manager Configs
     */
    public static final String RM_PREFIX = "yarn.resourcemanager.";

    /**
     * MR 任务的 Principal 信息，也可以认为是 Yarn 的 Principal
     */
    public static final String RM_PRINCIPAL = RM_PREFIX + "principal";
}
