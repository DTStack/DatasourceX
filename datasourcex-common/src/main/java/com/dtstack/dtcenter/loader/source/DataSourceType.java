/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dtstack.dtcenter.loader.source;

import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.dtstack.dtcenter.loader.utils.AssertUtils;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 10:32 2020/7/27
 * @Description：数据源类型 值 1000 以上表示未启用，后续标号 99 以下倒序的表示定制化的需求
 */
public enum DataSourceType {
    // RDBMS
    MySQL(1, 0, "MySQL", "MySQL", "mysql5"),
    MySQL8(1001, 1, "MySQL", "MySQL8", "mysql5"),
    MySQLPXC(98, 1, "MySQL PXC", "MySQL PXC", "mysql5"),
    MYSQL_SHARDING(86, 1, "MySQL SHARDING", "MySQL SHARDING", "mysql_sharding"),
    Polardb_For_MySQL(28, 2, "PolarDB for MySQL8", "PolarDB for MySQL8", "mysql5"),
    Oracle(2, 3, "Oracle", "Oracle", "oracle"),
    SQLServer(3, 4, "SQLServer", "SQLServer", "sqlServer"),
    SQLSERVER_2017_LATER(32, 5, "SQLServer JDBC", "SQLServer JDBC", "sqlServer"),
    PostgreSQL(4, 6, "PostgreSQL", "PostgreSQL", "postgresql"),
    DB2(19, 7, "DB2", "DB2", "db2"),
    DMDB(35, 8, "DMDB For MySQL", "DMDB For MySQL", "dmdb"),
    RDBMS(5, 9, "RDBMS", "RDBMS", "mysql"),
    KINGBASE8(40, 10, "KingbaseES8", "KingbaseES8", "kingbase8"),
    DMDB_For_Oracle(67, 8, "DMDB For Oracle", "DMDB For Orcale", "dmdb"),

    // Hadoop
    HIVE(7, 20, "Hive2.x", "Hive2.x", "hive"),
    HIVE1X(27, 21, "Hive1.x", "Hive1.x", "hive1"),
    HIVE3X(50, 22, "Hive3.x", "Hive3.x", "hive3"),
    MAXCOMPUTE(10, 23, "MaxCompute", "MaxCompute", "maxcompute"),

    // MPP
    GREENPLUM6(36, 40, "Greenplum", "Greenplum", "greenplum6"),
    LIBRA(21, 41, "GaussDB", "GaussDB", "libra"),
    GBase_8a(22, 42, "GBase_8a", "GBase_8a", "gbase"),
    DORIS(57, 43, "Doris0.14.x(jdbc)", "Doris0.14.x(jdbc)", "doris"),
    STAR_ROCKS(91, 44, "StarRocks", "StarRocks", "starrocks"),

    // FileSystem
    HDFS(6, 60, "HDFS", "HDFS", "hdfs"),
    HDFS3(63, 61, "HDFS3", "HDFS3", "hdfs3"),
    HDFS3_CDP(1003, 1003, "HDFS3_CDP", "HDFS3_CDP", "hdfs3_cdp"),
    FTP(9, 62, "FTP", "FTP", "ftp"),
    S3(41, 63, "S3", "S3", "s3"),
    AWS_S3(51, 64, "AWS S3", "AWS S3", "aws_s3"),

    // Analytic
    SparkThrift2_1(45, 80, "SparkThrift2.x", "SparkThrift2.x", "spark"),
    IMPALA(29, 81, "Impala", "Impala", "impala"),
    Clickhouse(25, 82, "ClickHouse", "ClickHouse", "clickhouse"),
    TiDB(31, 83, "TiDB", "TiDB", "tidb"),
    CarbonData(20, 84, "CarbonData", "CarbonData", "hive"),
    Kudu(24, 85, "Kudu", "Kudu", "kudu"),
    ADS(15, 86, "AnalyticDB", "AnalyticDB", "mysql5"),
    ADB_FOR_PG(54, 87, "AnalyticDB PostgreSQL", "AnalyticDB PostgreSQL", "postgresql"),
    Kylin(23, 88, "Kylin", "Kylin", "kylin"),
    Presto(48, 89, "Presto", "Presto", "presto"),
    OceanBase(49, 90, "OceanBase", "OceanBase", "oceanbase"),
    INCEPTOR(52, 91, "Inceptor", "Inceptor", "inceptor"),
    TRINO(59, 92, "Trino", "Trino", "trino"),
    ICEBERG(66, 93, "IceBerg", "IceBerg", "iceberg"),
    SAP_HANA1(76, 93, "SAP HANA 1.x", "SAP HANA 1.x", "sap_hana"),
    SAP_HANA2(77, 94, "SAP HANA 2.x", "SAP HANA 2.x", "sap_hana"),

    // NoSQL
    HBASE(8, 100, "HBase", "HBase", "hbase"),
    HBASE2(39, 101, "HBase", "HBase2", "hbase"),
    Phoenix(30, 102, "Phoenix4.x", "Phoenix4.x", "phoenix"),
    PHOENIX5(38, 103, "Phoenix5.x", "Phoenix5.x", "phoenix5"),
    ES(11, 104, "ElasticSearch5.x", "ElasticSearch5.x", "es5"),
    ES6(33, 105, "ElasticSearch6.x", "ElasticSearch6.x", "es"),
    ES7(46, 106, "ElasticSearch7.x", "ElasticSearch7.x", "es7"),
    MONGODB(13, 107, "MongoDB", "MongoDB", "mongo"),
    REDIS(12, 108, "Redis", "Redis", "redis"),
    SOLR(53, 109, "Solr", "Solr", "solr"),

    // others
    KAFKA_2X(37, 120, "Kafka2.x", "Kafka", "kafka"),
    KAFKA(26, 121, "Kafka", "Kafka", "kafka"),
    KAFKA_11(14, 122, "Kafka_0.11", "Kafka", "kafka"),
    KAFKA_10(17, 123, "Kafka_0.10", "Kafka", "kafka"),
    KAFKA_09(18, 124, "Kafka_0.9", "Kafka", "kafka"),
    Confluent5(79, 125, "confluent 5.x", "confluent", "confluent5"),
    EMQ(34, 126, "EMQ", "EMQ", "emq"),
    WEB_SOCKET(42, 127, "WebSocket", "WebSocket", "websocket"),
    SOCKET(44, 128, "Socket", "Socket", "socket"),
    RESTFUL(47, 129, "Restful", "Restful", "restful"),
    VERTICA(43, 130, "Vertica", "Vertica", "vertica"),
    VERTICA11(69, 130, "Vertica_11", "Vertica", "vertica"),
    INFLUXDB(55, 131, "InfluxDB", "InfluxDB", "influxdb"),
    OPENTSDB(56, 132, "OpenTSDB", "OpenTSDB", "opentsdb"),
    BEATS(16, 133, "Beats", "Beats", "null"),
    Spark(1002, 134, "Spark", "Spark", "spark"),
    KylinRestful(58, 135, "KylinRestful", "KylinRestful", "kylinrestful"),

    TBDS_HDFS(60, 136, "TBDS_HDFS", "TBDS_HDFS", "tbds_hdfs"),
    TBDS_HBASE(61, 137, "TBDS_HBASE", "TBDS_HBASE", "tbds_hbase"),
    TBDS_KAFKA(62, 138, "TBDS_KAFKA", "TBDS_KAFKA", "tbds_kafka"),
    DorisRestful(64, 139, "Doris0.14.x(http)", "Doris0.14.x(http)", "dorisrestful"),
    HIVE3_CDP(65, 140, "Hive3_CDP", "Hive3_CDP", "hive3_cdp"),

    DRDS(72, 145, "DRDS", "DRDS", "mysql5"),
    UPDRDB(73, 146, "UPDRDB", "UPDRDB", "mysql5"),
    UPRedis(74, 147, "UPRedis", "UPRedis", "redis"),
    CSP_S3(75, 148, "CSP S3", "CSP S3", "csp_s3"),

    HUAWEI_KAFKA(70, 143, "HUAWEI_KAFKA", "HUAWEI_KAFKA", "huawei_kafka"),
    HUAWEI_HBASE(71, 144, "HUAWEI_HBASE", "HUAWEI_HBASE", "huawei_hbase"),
    HUAWEI_HDFS(78, 145, "HUAWEI_HDFS", "HUAWEI_HDFS", "huawei_hdfs"),

    YARN2(80, 146, "YARN2", "YARN2", "yarn2"),
    YARN3(81, 147, "YARN3", "YARN3", "yarn3"),
    TBDS_YARN2(82, 148, "TBDS_YARN2", "TBDS_YARN2", "yarn2tbds"),
    HUAWEI_YARN3(83, 149, "HUAWEI_YARN3", "HUAWEI_YARN3", "yarn3hw"),
    KUBERNETES(84, 150, "KUBERNETES", "KUBERNETES", "kubernetes"),
    NFS(85, 151, "NFS", "NFS", "nfs"),
    TDENGINE(87, 152, "TDengine", "TDengine", "tdengine"),

    LDAP(90, 152, "LDAP", "LDAP", "ldap"),
    HIVE_CDC(88, 152, "HIVE_CDC", "HIVE_CDC", "hive_cdc"),

    HBASE_RESTFUL(99, 153, "HBASE_RESTFUL", "HBASE_RESTFUL", "hbase_restful"),
    // 增加 kerberos 处理
    KERBEROS(300, 300, "KERBEROS", "KERBEROS", "kerberos"),

    RANGER(89, 89, "RANGER", "RANGER", "ranger"),

    ROCKET_MQ(93,153, "ROCKET_MQ","ROCKET_MQ","rocket_mq"),

    RABBIT_MQ(92,154,"RABBIT_MQ","RABBIT_MQ","rabbit_mq")
    ;

    DataSourceType(int val, int order, String name, String groupTag, String pluginName) {
        this.val = val;
        this.order = order;
        this.name = name;
        this.groupTag = groupTag;
        this.pluginName = pluginName;
    }

    private static final List<Integer> RDBM_S = new ArrayList<>();
    private static final List<Integer> KAFKA_S = new ArrayList<>();
    public static final Map<String, List<Integer>> GROUP = new HashMap<>(DataSourceType.values().length);

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
        RDBM_S.add(HIVE3_CDP.val);
        RDBM_S.add(Spark.val);
        RDBM_S.add(SparkThrift2_1.val);
        RDBM_S.add(Presto.val);
        RDBM_S.add(Kylin.val);
        RDBM_S.add(VERTICA.val);
        RDBM_S.add(VERTICA11.val);
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
        RDBM_S.add(TRINO.val);
        RDBM_S.add(ICEBERG.val);
        RDBM_S.add(DMDB_For_Oracle.val);
        RDBM_S.add(DRDS.val);
        RDBM_S.add(UPDRDB.val);
        RDBM_S.add(SAP_HANA1.val);
        RDBM_S.add(SAP_HANA2.val);

        KAFKA_S.add(KAFKA.val);
        KAFKA_S.add(KAFKA_09.val);
        KAFKA_S.add(KAFKA_10.val);
        KAFKA_S.add(KAFKA_11.val);
        KAFKA_S.add(KAFKA_2X.val);
        KAFKA_S.add(TBDS_KAFKA.val);
        KAFKA_S.add(HUAWEI_KAFKA.val);
        KAFKA_S.add(Confluent5.val);

        for (DataSourceType dataSourceType : DataSourceType.values()) {
            List<Integer> types = DataSourceType.GROUP.get(dataSourceType.getGroupTag());
            if (null == types) {
                types = Lists.newArrayList();
            }
            types.add(dataSourceType.val);
            DataSourceType.GROUP.put(dataSourceType.getGroupTag(), types);
        }
    }


    /**
     * 数据源值
     */
    private final int val;

    /**
     * 排序顺序
     */
    private final int order;

    /**
     * 数据源名称
     */
    private final String name;

    /**
     * 数据源所属组
     */
    private final String groupTag;

    /**
     * 插件包目录名称
     */
    private final String pluginName;

    /**
     * 根据值获取数据源类型
     *
     * @param value 数据源插件 val
     * @return 数据源插件枚举
     */
    public static @NotNull DataSourceType getSourceType(int value) {
        for (DataSourceType type : DataSourceType.values()) {
            if (type.val == value) {
                return type;
            }
        }

        throw new DtLoaderException(String.format("Data source type[%s] is not supported", value));
    }

    /**
     * 根据值获取数据源类型
     *
     * @param pluginName 插件名称
     * @return 数据源类型
     */
    public static @NotNull DataSourceType getSourceTypeByPluginName(String pluginName) {
        AssertUtils.notBlank(pluginName, "plugin name can't be null.");
        for (DataSourceType type : DataSourceType.values()) {
            if (StringUtils.equalsIgnoreCase(type.pluginName, pluginName)) {
                return type;
            }
        }
        throw new DtLoaderException(String.format("Data source plugin[%s] is not supported", pluginName));
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

    public String getGroupTag() {
        return groupTag;
    }

    public int getOrder() {
        return order;
    }

    /**
     * 获取所有的关系数据库
     *
     * @return 所有关系型数据库的 val
     */
    public static List<Integer> getRDBMS() {
        return RDBM_S;
    }

    /**
     * 获取所有的 kafka 相关数据源
     *
     * @return 所有 kafka 相关数据源的 val
     */
    public static List<Integer> getKafkaS() {
        return KAFKA_S;
    }

    /**
     * 用来计算未使用的最小数据源值
     *
     * @param args main args
     */
    public static void main(String[] args) {
        List<DataSourceType> collect = Arrays.stream(DataSourceType.values()).sorted(Comparator.comparingInt(DataSourceType::getVal)).collect(Collectors.toList());
        // 利用 toMap 特性，校验 value 枚举值是否重复
        try {
            collect.stream().collect(Collectors.toMap(DataSourceType::getVal, Function.identity()));
        } catch (Exception e) {
            System.err.println(e);
            return;
        }
        int val = 1;
        for (DataSourceType dataSourceType : collect) {
            val = val == dataSourceType.getVal() - 1 ? dataSourceType.getVal() : val;
        }
        System.out.println("Sys.out.currentVal : " + (val + 1));
    }
}