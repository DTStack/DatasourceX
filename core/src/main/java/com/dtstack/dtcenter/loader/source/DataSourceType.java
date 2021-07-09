package com.dtstack.dtcenter.loader.source;

import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 10:32 2020/7/27
 * @Description：数据源类型 值 1000 以上表示未启用，后续标号 99 以下倒序的表示定制化的需求
 */
public enum DataSourceType {
    // RDBMS
    MySQL(1, 0, "MySQL", "mysql5"),
    MySQL8(1001, 1, "MySQL", "mysql5"),
    MySQLPXC(98, 1, "MySQL PXC", "mysql5"),
    Polardb_For_MySQL(28, 2, "PolarDB for MySQL8", "mysql5"),
    Oracle(2, 3, "Oracle", "oracle"),
    SQLServer(3, 4, "SQLServer", "sqlServer"),
    SQLSERVER_2017_LATER(32, 5, "SQLServer", "sqlServer"),
    PostgreSQL(4, 6, "PostgreSQL", "postgresql"),
    DB2(19, 7, "DB2", "db2"),
    DMDB(35, 8, "DMDB", "dmdb"),
    RDBMS(5, 9, "RDBMS", "mysql"),
    KINGBASE8(40, 10, "KingbaseES8", "kingbase8"),

    // Hadoop
    HIVE(7, 20, "Hive2.x", "hive"),
    HIVE1X(27, 21, "Hive1.x", "hive1"),
    HIVE3X(50, 22, "Hive3.x", "hive3"),
    MAXCOMPUTE(10, 23, "MaxCompute", "maxcompute"),

    // MPP
    GREENPLUM6(36, 40, "Greenplum", "greenplum6"),
    LIBRA(21, 41, "GaussDB", "libra"),
    GBase_8a(22, 42, "GBase_8a", "gbase"),
    DORIS(57, 43, "DorisDB", "doris"),

    // FileSystem
    HDFS(6, 60, "HDFS", "hdfs"),
    FTP(9, 61, "FTP", "ftp"),
    S3(41, 62, "S3", "s3"),
    AWS_S3(51, 63, "AWS S3", "aws_s3"),

    // Analytic
    SparkThrift2_1(45, 80, "SparkThrift2.x", "spark"),
    IMPALA(29, 81, "Impala", "impala"),
    Clickhouse(25, 82, "ClickHouse", "clickhouse"),
    TiDB(31, 83, "TiDB", "mysql5"),
    CarbonData(20, 84, "CarbonData", "hive"),
    Kudu(24, 85, "Kudu", "kudu"),
    ADS(15, 86, "AnalyticDB", "mysql5"),
    ADB_FOR_PG(54, 87, "AnalyticDB PostgreSQL", "postgresql"),
    Kylin(23, 88, "Kylin", "kylin"),
    Presto(48, 89, "Presto", "presto"),
    OceanBase(49, 90, "OceanBase", "oceanbase"),
    INCEPTOR(52, 91, "Inceptor", "inceptor"),

    // NoSQL
    HBASE(8, 100, "HBase", "hbase"),
    HBASE2(39, 101, "HBase", "hbase"),
    Phoenix(30, 102, "Phoenix4.x", "phoenix"),
    PHOENIX5(38, 103, "Phoenix5.x", "phoenix5"),
    ES(11, 104, "ElasticSearch5.x", "es"),
    ES6(33, 105, "ElasticSearch6.x", "es"),
    ES7(46, 106, "ElasticSearch7.x", "es"),
    MONGODB(13, 107, "MongoDB", "mongo"),
    REDIS(12, 108, "Redis", "redis"),
    SOLR(53, 109, "Solr", "solr"),
    //FIXME 临时增加，适配gateway上线，排除hadoop和hbase依赖，下版本删除
    HBASE_GATEWAY(99, 109, "HBase1.x", "hbase_gateway"),

    // others
    KAFKA_2X(37, 120, "Kafka2.x", "kafka"),
    KAFKA(26, 121, "Kafka", "kafka"),
    KAFKA_11(14, 122, "Kafka_0.11", "kafka"),
    KAFKA_10(17, 123, "Kafka_0.10", "kafka"),
    KAFKA_09(18, 124, "Kafka_0.9", "kafka"),
    EMQ(34, 125, "EMQ", "emq"),
    WEB_SOCKET(42, 126, "WebSocket", "websocket"),
    SOCKET(44, 127, "Socket", "socket"),
    RESTFUL(47, 128, "Restful", "restful"),
    VERTICA(43, 129, "Vertica", "vertica"),
    INFLUXDB(55, 130, "InfluxDB", "influxdb"),
    OPENTSDB(56, 131, "OpenTSDB", "opentsdb"),
    BEATS(16, 132, "Beats", "null"),
    Spark(1002, 133, "Spark", "spark"),
    ;

    DataSourceType(int val, int order, String name, String pluginName) {
        this.val = val;
        this.order = order;
        this.name = name;
        this.pluginName = pluginName;
    }

    private static final List<Integer> RDBM_S = new ArrayList<>();
    private static final List<Integer> KAFKA_S = new ArrayList<>();

    static {
        RDBM_S.add(MySQL.val);
        RDBM_S.add(MySQL8.val);
        RDBM_S.add(MySQLPXC.val);
        RDBM_S.add(Polardb_For_MySQL.val);
        RDBM_S.add(Oracle.val);
        RDBM_S.add(SQLServer.val);
        RDBM_S.add(SQLSERVER_2017_LATER.val);
        RDBM_S.add(PostgreSQL.val);
        RDBM_S.add(DB2.val);
        RDBM_S.add(DMDB.val);
        RDBM_S.add(RDBMS.val);
        RDBM_S.add(HIVE.val);
        RDBM_S.add(HIVE1X.val);
        RDBM_S.add(HIVE3X.val);
        RDBM_S.add(GREENPLUM6.val);
        RDBM_S.add(LIBRA.val);
        RDBM_S.add(GBase_8a.val);
        RDBM_S.add(Clickhouse.val);
        RDBM_S.add(TiDB.val);
        RDBM_S.add(CarbonData.val);
        RDBM_S.add(ADS.val);
        RDBM_S.add(ADB_FOR_PG.val);
        RDBM_S.add(Phoenix.val);
        RDBM_S.add(PHOENIX5.val);
        RDBM_S.add(IMPALA.val);
        RDBM_S.add(OceanBase.val);
        RDBM_S.add(INCEPTOR.val);
        RDBM_S.add(KINGBASE8.val);

        KAFKA_S.add(KAFKA.val);
        KAFKA_S.add(KAFKA_09.val);
        KAFKA_S.add(KAFKA_10.val);
        KAFKA_S.add(KAFKA_11.val);
        KAFKA_S.add(KAFKA_2X.val);
    }

    /**
     * 数据源值
     */
    private int val;

    /**
     * 排序顺序
     */
    private int order;

    /**
     * 数据源名称
     */
    private String name;

    private String pluginName;

    /**
     * 根据值获取数据源类型
     *
     * @param value
     * @return
     */
    public static @NotNull DataSourceType getSourceType(int value) {
        for (DataSourceType type : DataSourceType.values()) {
            if (type.val == value) {
                return type;
            }
        }

        throw new DtLoaderException("Data source type is not supported");
    }

    public Integer getVal() {
        return val;
    }

    public String getName() {
        return name;
    }

    public String getPluginName() {
        return pluginName;
    }

    public int getOrder() {
        return order;
    }

    /**
     * 获取所有的关系数据库
     *
     * @return
     */
    public static List<Integer> getRDBMS() {
        return RDBM_S;
    }

    /**
     * 获取所有的 kafka 相关数据源
     *
     * @return
     */
    public static List<Integer> getKafkaS() {
        return KAFKA_S;
    }

    /**
     * 用来计算未使用的最小数据源值
     *
     * @param args
     */
    public static void main(String[] args) {
        List<DataSourceType> collect = Arrays.stream(DataSourceType.values()).sorted(Comparator.comparingInt(DataSourceType::getVal)).collect(Collectors.toList());
        int val = 1;
        for (DataSourceType dataSourceType : collect) {
            val = val == dataSourceType.getVal() - 1 ? dataSourceType.getVal() : val;
        }
        System.out.println("Sys.out.currentVal : " + (val + 1));
    }
}