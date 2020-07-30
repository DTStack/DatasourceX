package com.dtstack.dtcenter.loader.source;

import com.dtstack.dtcenter.common.exception.DtCenterDefException;
import org.jetbrains.annotations.NotNull;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 10:32 2020/7/27
 * @Description：数据源类型 值 1000 以上表示未启用，后续标号
 */
public enum DataSourceType {
    MySQL(1, "MySQL", "mysql5"),
    MySQL8(1001, "MySQL8", "mysql8"),
    Oracle(2, "Oracle", "oracle"),
    SQLServer(3, "SQLServer", "sqlServer"),
    SQLSERVER_2017_LATER(32, "SQLServer2017_and_later", "sqlServer2017"),
    PostgreSQL(4, "PostgreSQL", "postgresql"),
    RDBMS(5, "RDBMS", "mysql"),
    HDFS(6, "HDFS", "hdfs"),
    HIVE(7, "Hive2.x", "hive"),
    HBASE(8, "HBase1.x", "hbase"),
    FTP(9, "FTP", "ftp"),
    MAXCOMPUTE(10, "MaxCompute", "maxcompute"),
    ES(11, "ElasticSearch5.x", "es"),
    REDIS(12, "Redis", "redis"),
    MONGODB(13, "MongoDB", "mongo"),
    KAFKA_11(14, "Kafka_0.11", "kafka"),
    ADS(15, "AnalyticDB", "mysql5"),
    BEATS(16, "Beats", "null"),
    KAFKA_10(17, "Kafka_0.10", "kafka"),
    KAFKA_09(18, "Kafka_0.9", "kafka"),
    DB2(19, "DB2", "db2"),
    CarbonData(20, "CarbonData", "hive"),
    LIBRA(21, "LibrA", "libra"),
    GBase_8a(22, "GBase_8a", "gbase"),
    Kylin(23, "Kylin", "kylin"),
    Kudu(24, "Kudu", "kudu"),
    Clickhouse(25, "ClickHouse", "clickhouse"),
    KAFKA(26, "Kafka", "kafka"),
    HIVE1X(27, "Hive1.x", "hive1"),
    Polardb_For_MySQL(28, "PolarDB for MySQL8", "mysql5"),
    IMPALA(29, "Impala", "impala"),
    Phoenix(30, "Phoenix4.x", "phoenix"),
    TiDB(31, "TiDB", "mysql5"),
    ES6(33, "ElasticSearch6.x", "es"),
    EMQ(34, "EMQ", "emq"),
    DMDB(35, "DMDB", "dmdb"),
    GREENPLUM6(36, "Greenplum", "greenplum6"),
    KAFKA_2X(37, "Kafka2.x", "kafka"),
    PHOENIX5(38, "Phoenix5.x", "phoenix5"),
    HBASE2(39, "HBase2.x", "hbase2"),
    ;

    DataSourceType(int val, String name, String pluginName) {
        this.val = val;
        this.name = name;
        this.pluginName = pluginName;
    }

    /**
     * 数据源值
     */
    private int val;

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

        throw new DtCenterDefException("不支持数据源类型");
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
}