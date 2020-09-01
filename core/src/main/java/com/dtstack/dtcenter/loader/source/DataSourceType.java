package com.dtstack.dtcenter.loader.source;

import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 10:32 2020/7/27
 * @Description：数据源类型 值 1000 以上表示未启用，后续标号
 */
public enum DataSourceType {
    // RDBMS
    MySQL(1, 0, "MySQL", "mysql5"),
    MySQL8(1001, 1, "MySQL8", "mysql8"),
    Polardb_For_MySQL(28, 2, "PolarDB for MySQL8", "mysql5"),
    Oracle(2, 3, "Oracle", "oracle"),
    SQLServer(3, 4, "SQLServer", "sqlServer"),
    SQLSERVER_2017_LATER(32, 5, "SQLServer2017_and_later", "sqlServer2017"),
    PostgreSQL(4, 6, "PostgreSQL", "postgresql"),
    DB2(19, 7, "DB2", "db2"),
    DMDB(35, 8, "DMDB", "dmdb"),
    RDBMS(5, 9, "RDBMS", "mysql"),

    // Hadoop
    HIVE(7, 20, "Hive2.x", "hive"),
    HIVE1X(27, 21, "Hive1.x", "hive1"),
    MAXCOMPUTE(10, 22, "MaxCompute", "maxcompute"),

    // MPP
    GREENPLUM6(36, 40, "Greenplum", "greenplum6"),
    LIBRA(21, 41, "LibrA", "libra"),
    GBase_8a(22, 42, "GBase_8a", "gbase"),

    // UnStructed
    HDFS(6, 60, "HDFS", "hdfs"),
    FTP(9, 61, "FTP", "ftp"),

    // Analytic
    IMPALA(29, 80, "Impala", "impala"),
    Clickhouse(25, 81, "ClickHouse", "clickhouse"),
    TiDB(31, 82, "TiDB", "mysql5"),
    CarbonData(20, 83, "CarbonData", "hive"),
    Kudu(24, 84, "Kudu", "kudu"),
    ADS(15, 85, "AnalyticDB", "mysql5"),
    Kylin(23, 86, "Kylin", "kylin"),

    // NoSQL
    HBASE(8, 100, "HBase1.x", "hbase"),
    HBASE2(39, 101, "HBase2.x", "hbase2"),
    Phoenix(30, 102, "Phoenix4.x", "phoenix"),
    PHOENIX5(38, 103, "Phoenix5.x", "phoenix5"),
    ES(11, 104, "ElasticSearch5.x", "es"),
    ES6(33, 105, "ElasticSearch6.x", "es"),
    MONGODB(13, 106, "MongoDB", "mongo"),
    REDIS(12, 107, "Redis", "redis"),

    // others
    KAFKA_2X(37, 120, "Kafka2.x", "kafka"),
    KAFKA(26, 121, "Kafka", "kafka"),
    KAFKA_11(14, 122, "Kafka_0.11", "kafka"),
    KAFKA_10(17, 123, "Kafka_0.10", "kafka"),
    KAFKA_09(18, 124, "Kafka_0.9", "kafka"),
    EMQ(34, 125, "EMQ", "emq"),
    BEATS(16, 126, "Beats", "null"),
    Spark(1002, 127, "Spark", "spark"),
    ;

    DataSourceType(int val, int order, String name, String pluginName) {
        this.val = val;
        this.order = order;
        this.name = name;
        this.pluginName = pluginName;
    }

    private static List<Integer> rdbms = new ArrayList<>();

    static {
        rdbms.add(MySQL.val);
        rdbms.add(MySQL8.val);
        rdbms.add(Polardb_For_MySQL.val);
        rdbms.add(Oracle.val);
        rdbms.add(SQLServer.val);
        rdbms.add(SQLSERVER_2017_LATER.val);
        rdbms.add(PostgreSQL.val);
        rdbms.add(DB2.val);
        rdbms.add(DMDB.val);
        rdbms.add(RDBMS.val);
        rdbms.add(HIVE.val);
        rdbms.add(HIVE1X.val);
        rdbms.add(GREENPLUM6.val);
        rdbms.add(LIBRA.val);
        rdbms.add(GBase_8a.val);
        rdbms.add(Clickhouse.val);
        rdbms.add(TiDB.val);
        rdbms.add(CarbonData.val);
        rdbms.add(ADS.val);
        rdbms.add(Phoenix.val);
        rdbms.add(PHOENIX5.val);

        rdbms = rdbms.stream().distinct().collect(Collectors.toList());
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

        throw new DtLoaderException("不支持数据源类型");
    }

    public int getVal() {
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
        return rdbms;
    }
}