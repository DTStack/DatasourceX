package com.dtstack.dtcenter.loader.enums;

import com.dtstack.dtcenter.common.enums.DataSourceType;
import com.dtstack.dtcenter.common.exception.DtCenterDefException;

import java.util.Arrays;
import java.util.Optional;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 11:20 2020/2/27
 * @Description：数据源插件类型
 */
public enum DataSourceClientType {

    /**
     * MySql 相关
     */
    MySql5(DataSourceType.MySQL.getVal(), "mysql5"),
    MySql8(DataSourceType.MySQL8.getVal(), "mysql8"),
    MAXCOMPUTE(DataSourceType.MAXCOMPUTE.getVal(), "maxcompute"),
    ADS(DataSourceType.ADS.getVal(), "mysql5"),
    Polardb_For_MySQL(DataSourceType.Polardb_For_MySQL.getVal(), "mysql5"),
    TiDB(DataSourceType.TiDB.getVal(), "mysql5"),

    /**
     * ORACLE 相关
     */
    Oracle(DataSourceType.Oracle.getVal(), "oracle"),

    /**
     * SQLServer 相关
     */
    SQLServer(DataSourceType.SQLServer.getVal(), "sqlServer"),
    SQLSERVER_2017_LATER(DataSourceType.SQLSERVER_2017_LATER.getVal(), "sqlServer2017"),

    /**
     * Phoenix 相关
     * Phoenix 为默认的插件，如果只是单版本的，直接替换这个包，多个版本则需要搞多个 MODULE
     */
    Phoenix(DataSourceType.Phoenix.getVal(), "phoenix"),

    /**
     * DB2 相关
     */
    DB2(DataSourceType.DB2.getVal(), "db2"),

    /**
     * PostgreSQL 相关
     */
    PostgreSQL(DataSourceType.PostgreSQL.getVal(), "postgresql"),

    /**
     * Hadoop 相关
     */
    HDFS(DataSourceType.HDFS.getVal(), "hdfs"),
    HIVE(DataSourceType.HIVE.getVal(), "hive"),
    HIVE1X(DataSourceType.HIVE1X.getVal(), "hive1"),
    HBASE(DataSourceType.HBASE.getVal(), "hbase"),
    CarbonData(DataSourceType.CarbonData.getVal(), "hive"),
    GBase_8a(DataSourceType.GBase_8a.getVal(), "gbase"),
    Kylin(DataSourceType.Kylin.getVal(), "kylin"),
    Kudu(DataSourceType.Kudu.getVal(), "kudu"),
    Clickhouse(DataSourceType.Clickhouse.getVal(), "clickhouse"),
    IMPALA(DataSourceType.IMPALA.getVal(), "impala"),

    /**
     * LIBRA 相关
     */
    LIBRA(DataSourceType.LIBRA.getVal(), "libra"),

    /**
     * FTP 相关
     */
    FTP(DataSourceType.FTP.getVal(), "ftp"),

    /**
     * ES 相关
     */
    ES(DataSourceType.ES.getVal(), "es"),
    ES6(DataSourceType.ES6.getVal(), "es"),

    /**
     * NOSQL 相关
     */
    REDIS(DataSourceType.REDIS.getVal(), "redis"),
    MONGODB(DataSourceType.MONGODB.getVal(), "mongo"),

    /**
     * KAFKA 相关
     */
    KAFKA_11(DataSourceType.KAFKA_11.getVal(), "kafka"),
    KAFKA_10(DataSourceType.KAFKA_10.getVal(), "kafka"),
    KAFKA_09(DataSourceType.KAFKA_09.getVal(), "kafka"),
    KAFKA(DataSourceType.KAFKA.getVal(), "kafka"),

    ;


    private Integer sourceType;

    private String pluginName;

    DataSourceClientType(Integer sourceType, String pluginName) {
        this.sourceType = sourceType;
        this.pluginName = pluginName;
    }

    /**
     * 根据数据源类型获取插件包名称
     *
     * @param sourceType
     * {@link DataSourceType#getVal()}
     * @return
     */
    public static String getPluginNameByType(Integer sourceType) {
        Optional<DataSourceClientType> dataSourceClientType =
                Arrays.stream(DataSourceClientType.values()).filter(obj -> obj.sourceType.equals(sourceType)).findFirst();
        if (dataSourceClientType.isPresent()) {
            return dataSourceClientType.get().pluginName;
        }
        throw new DtCenterDefException("为支持的数据源");
    }
}
