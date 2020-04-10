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

        public static final String LINE_SEPARATOR = "\n";

        /**
         * 数据库中对应关系字段
         */
        public static final String KEY = "key";
        public static final String NAME = "name";
        public static final String TYPE = "type";
        public static final String COMMENT = "comment";
        public static final String IS_PART = "isPart";
        public static final String COL_NAME = "col_name";
        public static final String DATA_TYPE = "data_type";
        public static final String PRIMARY_KEY = "primary_key";
        public static final String REMARKS = "REMARKS";

        public static final String USE_DB = "use `%s`";
    }

    class PatternConsistent {
        /**
         * JDBC 正则
         */
        public static Pattern JDBC_PATTERN = Pattern.compile("(?i)jdbc:[a-zA-Z0-9\\.]+://(?<host>[0-9a-zA-Z\\.-]+):(?<port>\\d+)/(?<db>[0-9a-zA-Z_%\\.]+)(?<param>[\\?;#].*)*");
        /**
         * HIVE_JDBC_URL 正则解析
         */
        public static final Pattern HIVE_JDBC_PATTERN = Pattern.compile("(?i)jdbc:hive2://(?<url>[0-9a-zA-Z,\\:\\-\\.]+)(/(?<db>[0-9a-z_%]+)*(?<param>[\\?;#].*)*)*");

        public static final Pattern IMPALA_JDBC_PATTERN = Pattern.compile("(?i)jdbc:impala://[0-9a-zA-Z\\-\\.]+:[\\d]+/(?<db>[0-9a-zA-Z\\-]+);.*");

        public static final Pattern GREENPLUM_JDBC_PATTERN = Pattern.compile("(?i)jdbc:pivotal:greenplum://[0-9a-zA-Z\\-\\.]+:[\\d]+;DatabaseName=(?<db>[0-9a-zA-Z\\-]+);.*");
    }

    class HadoopConfConsistent {
        public static final String HADOOP_CONFIG = "hadoopConfig";

        public static final String DEFAULT_FS_REGEX = "hdfs://.*";

        public static final String TABLE_INFORMATION = "# Detailed Table Information";

        public static final String COMMENT = "Comment:";

        public static final String DESCRIBE_EXTENDED = "describe extended %s";
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
