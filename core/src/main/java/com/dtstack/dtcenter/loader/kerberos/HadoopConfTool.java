package com.dtstack.dtcenter.loader.kerberos;

import java.util.Arrays;
import java.util.List;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 20:57 2020/8/27
 * @Description：Hadoop 配置中心
 */
public class HadoopConfTool {
    /**
     * krb5 系统属性键
     */
    public static final String KEY_JAVA_SECURITY_KRB5_CONF = "java.security.krb5.conf";

    /**
     * principal 键
     */
    public static final String PRINCIPAL = "principal";

    /**
     * principal 文件 键
     */
    public static final String PRINCIPAL_FILE = "principalFile";

    /**
     * Kafka kerberos keytab 键
     */
    public static final String KAFKA_KERBEROS_KEYTAB = "kafka.kerberos.keytab";

    /**
     * Principal 所有的 键
     */
    public static List<String> PRINCIPAL_KEYS = Arrays.asList(
            "hive.server2.authentication.kerberos.principal",
            "hive.metastore.kerberos.principal",
            "beeline.hs2.connection.principal",
            "yarn.resourcemanager.principal",
            "yarn.nodemanager.principal",
            "dfs.namenode.kerberos.principal",
            "dfs.datanode.kerberos.principal",
            "dfs.journalnode.kerberos.principal"
    );
}
