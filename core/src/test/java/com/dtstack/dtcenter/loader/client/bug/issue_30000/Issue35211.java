package com.dtstack.dtcenter.loader.client.bug.issue_30000;

import com.dtstack.dtcenter.loader.IDownloader;
import com.dtstack.dtcenter.loader.cache.pool.config.PoolConfig;
import com.dtstack.dtcenter.loader.client.ClientCache;
import com.dtstack.dtcenter.loader.client.IClient;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.source.HiveSourceDTO;
import com.dtstack.dtcenter.loader.dto.source.SparkSourceDTO;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import com.google.common.collect.Lists;
import org.apache.commons.collections.CollectionUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

/**
 * 修复 hive orc 格式表 array 字段数据下载时数组越界异常
 *
 * @author ：wangchuan
 * date：Created in 下午7:41 2021/4/7
 * company: www.dtstack.com
 */
public class Issue35211 {

    /**
     * 构造hive客户端
     */
    private static final IClient HIVE_CLIENT = ClientCache.getClient(DataSourceType.HIVE.getVal());

    /**
     * 构造spark客户端
     */
    private static final IClient SPARK_CLIENT = ClientCache.getClient(DataSourceType.Spark.getVal());

    /**
     * 构建数据源信息
     */
    private static final HiveSourceDTO HIVE_SOURCE_DTO = HiveSourceDTO.builder()
            .url("jdbc:hive2://172.16.100.214:10000/default")
            .schema("default")
            .defaultFS("hdfs://ns1")
            .username("admin")
            .config("{\n" +
                    "    \"dfs.ha.namenodes.ns1\": \"nn1,nn2\",\n" +
                    "    \"dfs.namenode.rpc-address.ns1.nn2\": \"172.16.101.227:9000\",\n" +
                    "    \"dfs.client.failover.proxy.provider.ns1\": \"org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider\",\n" +
                    "    \"dfs.namenode.rpc-address.ns1.nn1\": \"172.16.101.196:9000\",\n" +
                    "    \"dfs.nameservices\": \"ns1\"\n" +
                    "}")
            .poolConfig(PoolConfig.builder().build())
            .build();

    /**
     * 构建数据源信息
     */
    private static final SparkSourceDTO SPARK_SOURCE_DTO = SparkSourceDTO.builder()
            .url("jdbc:hive2://172.16.100.214:10000/default")
            .schema("default")
            .defaultFS("hdfs://ns1")
            .username("admin")
            .config("{\n" +
                    "    \"dfs.ha.namenodes.ns1\": \"nn1,nn2\",\n" +
                    "    \"dfs.namenode.rpc-address.ns1.nn2\": \"172.16.101.227:9000\",\n" +
                    "    \"dfs.client.failover.proxy.provider.ns1\": \"org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider\",\n" +
                    "    \"dfs.namenode.rpc-address.ns1.nn1\": \"172.16.101.196:9000\",\n" +
                    "    \"dfs.nameservices\": \"ns1\"\n" +
                    "}")
            .poolConfig(PoolConfig.builder().build())
            .build();

    /**
     * 数据准备
     */
    @BeforeClass
    public static void pre () {
        String dropOrcTableSql = "drop table if exists loader_bug_35211_orc";
        HIVE_CLIENT.executeSqlWithoutResultSet(HIVE_SOURCE_DTO, SqlQueryDTO.builder().sql(dropOrcTableSql).build());
        String createOrcTableSql = "create table loader_bug_35211_orc (id int,name array<string>) stored as orc";
        HIVE_CLIENT.executeSqlWithoutResultSet(HIVE_SOURCE_DTO, SqlQueryDTO.builder().sql(createOrcTableSql).build());
        List<String> arrayValues = Lists.newArrayList();
        for (int i = 0; i < 1025; i++) {
            arrayValues.add(String.format("'%s'", i));
        }
        String arrayString = String.join(",", arrayValues);
        String insertOrcTableSql = "insert into loader_bug_35211_orc values (1, array("+ arrayString +"))";
        HIVE_CLIENT.executeSqlWithoutResultSet(HIVE_SOURCE_DTO, SqlQueryDTO.builder().sql(insertOrcTableSql).build());
    }

    @Test
    public void test_for_issue () throws Exception {
        IDownloader hiveDownloader = HIVE_CLIENT.getDownloader(HIVE_SOURCE_DTO, SqlQueryDTO.builder().tableName("loader_bug_35211_orc").build());
        while (!hiveDownloader.reachedEnd()) {
            List<String> row = (List<String>) hiveDownloader.readNext();
            Assert.assertTrue(CollectionUtils.isNotEmpty(row));
        }
        IDownloader sparkDownloader = SPARK_CLIENT.getDownloader(SPARK_SOURCE_DTO, SqlQueryDTO.builder().tableName("loader_bug_35211_orc").build());
        while (!sparkDownloader.reachedEnd()) {
            List<String> row = (List<String>) sparkDownloader.readNext();
            Assert.assertTrue(CollectionUtils.isNotEmpty(row));
        }
    }
}
