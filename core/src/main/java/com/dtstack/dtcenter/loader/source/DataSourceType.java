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
    MySQL(1, "MySQL"),
    MySQL8(1001, "MySQL8"),
    Oracle(2, "Oracle"),
    SQLServer(3, "SQLServer"),
    SQLSERVER_2017_LATER(32, "SQLServer2017_and_later"),
    PostgreSQL(4, "PostgreSQL"),
    RDBMS(5, "RDBMS"),
    HDFS(6, "HDFS"),
    HIVE(7, "Hive"),
    HBASE(8, "HBase1.x"),
    FTP(9, "FTP"),
    MAXCOMPUTE(10, "MaxCompute"),
    ES(11, "ElasticSearch5.x"),
    REDIS(12, "Redis"),
    MONGODB(13, "MongoDB"),
    KAFKA_11(14, "Kafka_0.11"),
    ADS(15, "AnalyticDB"),
    BEATS(16, "Beats"),
    KAFKA_10(17, "Kafka_0.10"),
    KAFKA_09(18, "Kafka_0.9"),
    DB2(19, "DB2"),
    CarbonData(20, "CarbonData"),
    LIBRA(21, "LibrA"),
    GBase_8a(22, "GBase_8a"),
    Kylin(23, "Kylin"),
    Kudu(24, "Kudu"),
    Clickhouse(25, "ClickHouse"),
    KAFKA(26, "Kafka"),
    HIVE1X(27, "Hive1.x"),
    Polardb_For_MySQL(28, "PolarDB for MySQL8"),
    IMPALA(29, "Impala"),
    Phoenix(30, "Phoenix4.x"),
    TiDB(31, "TiDB"),
    ES6(33, "ElasticSearch6.x"),
    EMQ(34, "EMQ"),
    DMDB(35, "DMDB"),
    GREENPLUM6(36, "Greenplum"),
    KAFKA_2X(37, "Kafka2.x"),
    PHOENIX5(38, "Phoenix5.x"),
    HBASE2(39, "HBase2.x"),
    ;

    DataSourceType(int val, String name) {
        this.val = val;
        this.name = name;
    }

    /**
     * 数据源值
     */
    private int val;

    /**
     * 数据源名称
     */
    private String name;

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
}