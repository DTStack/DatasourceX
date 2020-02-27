package com.dtstack.dtcenter.loader;

import java.util.regex.Pattern;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 01:37 2020/2/27
 * @Description：常量
 */
public interface DtClassConsistent {
    class PublicConsistent {
        /**
         * 用户名
         */
        public static final String USER_NAME = "userName";

        /**
         * 密码
         */
        public static final String PWD = "pwd";

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

    class PatternConsistent {
        /**
         * HIVE_JDBC_URL 正则解析
         */
        public static final Pattern HIVE_JDBC_PATTERN = Pattern.compile("(?i)jdbc:hive2://(?<host>[0-9a-zA-Z\\-\\.]+)" +
                ":(?<port>\\d+)(/(?<db>[0-9a-z_%]+)*(?<param>[\\?;#].*)*)*");
    }

    class HadoopConfConsistent {
        public static final String HADOOP_CONFIG = "hadoopConfig";

        public static final String DEFAULT_FS_REGEX = "hdfs://.*";
    }

    class HBaseConsistent {
        /**
         * HBase 集群 根目录
         */
        public static final String KEY_ZOOKEEPER_ZNODE_PARENT = "zookeeper.znode.parent";

        /**
         * HBase 集群地址 KEY
         */
        public static final String KEY_HBASE_ZOOKEEPER_QUORUM = "hbase.zookeeper.quorum";
    }
}
