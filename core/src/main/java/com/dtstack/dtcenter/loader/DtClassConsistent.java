package com.dtstack.dtcenter.loader;

import java.util.regex.Pattern;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 01:37 2020/2/27
 * @Description：常量
 */
public interface DtClassConsistent {
    public static class PublicConsistent {
        /**
         * URL
         */
        public static final String HOST_KEY = "host";

        /**
         * 端口
         */
        public static final String PORT_KEY = "port";

        /**
         * 数据库 schema
         */
        public static final String DB_KEY = "db";

        /**
         * 其他参数
         */
        public static final String PARAM_KEY = "param";
    }

    public static class PatternConsistent {
        /**
         * HIVE_JDBC_URL 正则解析
         */
        public static final Pattern HIVE_JDBC_PATTERN = Pattern.compile("(?i)jdbc:hive2://(?<host>[0-9a-zA-Z\\-\\.]+)" +
                ":(?<port>\\d+)(/(?<db>[0-9a-z_%]+)*(?<param>[\\?;#].*)*)*");
    }

    public static class HadoopConfConsistent {
        /**
         * Hadoop principal
         */
        public final static String PRINCIPAL = "principal";

        /**
         * Hadoop principalFile
         */
        public final static String PRINCIPAL_FILE = "principalFile";

        /**
         * Hadoop 是否开启鉴权
         */
        public static final String IS_HADOOP_AUTHORIZATION = "hadoop.security.authorization";

        public final static String KEY_HBASE_MASTER_KERBEROS_PRINCIPAL = "hbase.master.kerberos.principal";

        public final static String KEY_HBASE_MASTER_KEYTAB_FILE = "hbase.master.keytab.file";

        public final static String KEY_JAVA_SECURITY_KRB5_CONF = "java.security.krb5.conf";

        public static final String DFS_NAMENODE_KEYTAB_FILE = "dfs.namenode.keytab.file";

        public static final String DFS_NAMENODE_KERBEROS_PRINCIPAL = "dfs.namenode.kerberos.principal";

        public final static String KEYTAB_PATH = "keytabPath";
    }
}
