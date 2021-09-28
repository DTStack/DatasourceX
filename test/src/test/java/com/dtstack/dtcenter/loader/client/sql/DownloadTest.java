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

package com.dtstack.dtcenter.loader.client.sql;

import com.dtstack.dtcenter.loader.IDownloader;
import com.dtstack.dtcenter.loader.cache.pool.config.PoolConfig;
import com.dtstack.dtcenter.loader.client.BaseTest;
import com.dtstack.dtcenter.loader.client.ClientCache;
import com.dtstack.dtcenter.loader.client.IClient;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.source.Hive1SourceDTO;
import com.dtstack.dtcenter.loader.dto.source.HiveSourceDTO;
import com.dtstack.dtcenter.loader.dto.source.SparkSourceDTO;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import com.google.common.collect.Lists;
import org.apache.commons.collections.CollectionUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * 数据下载测试
 *
 * @author ：wangchuan
 * date：Created in 3:29 下午 2021/2/26
 * company: www.dtstack.com
 */
public class DownloadTest extends BaseTest {

    /**
     * 构造hive2客户端
     */
    private static final IClient HIVE_CLIENT = ClientCache.getClient(DataSourceType.HIVE.getVal());

    /**
     * 构造hivex客户端
     */
    private static final IClient HIVE1_CLIENT = ClientCache.getClient(DataSourceType.HIVE1X.getVal());

    /**
     * 构造spark客户端
     */
    private static final IClient SPARK_CLIENT = ClientCache.getClient(DataSourceType.Spark.getVal());

    /**
     * 构建hive2数据源信息
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
     * 构建hive1数据源信息
     */
    private static final Hive1SourceDTO HIVE_1_SOURCE_DTO = Hive1SourceDTO.builder()
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
     * 构建spark数据源信息
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
    public static void beforeClass()  {
        System.setProperty("HADOOP_USER_NAME", "admin");
        // 创建 textfile 存储格式的表
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("drop table if exists loader_test_downloader_text").build();
        HIVE_CLIENT.executeSqlWithoutResultSet(HIVE_SOURCE_DTO, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("create table loader_test_downloader_text (id int comment 'ID', name string comment '姓名_name') partitioned by (pt string) row format delimited fields terminated by ','  stored as textfile ").build();
        HIVE_CLIENT.executeSqlWithoutResultSet(HIVE_SOURCE_DTO, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("insert into loader_test_downloader_text partition (pt = '2020') values (1, 'loader_test_1')").build();
        HIVE_CLIENT.executeSqlWithoutResultSet(HIVE_SOURCE_DTO, queryDTO);

        // 创建 parquet 存储格式的表
        queryDTO = SqlQueryDTO.builder().sql("drop table if exists loader_test_downloader_parquet").build();
        HIVE_CLIENT.executeSqlWithoutResultSet(HIVE_SOURCE_DTO, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("create table loader_test_downloader_parquet (id int comment 'ID', name string comment '姓名_name') partitioned by (pt string) stored as parquet").build();
        HIVE_CLIENT.executeSqlWithoutResultSet(HIVE_SOURCE_DTO, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("insert into loader_test_downloader_parquet partition (pt = '2020') values (1, 'loader_test_1')").build();
        HIVE_CLIENT.executeSqlWithoutResultSet(HIVE_SOURCE_DTO, queryDTO);

        // 创建 orc 存储格式的表
        queryDTO = SqlQueryDTO.builder().sql("drop table if exists loader_test_downloader_orc").build();
        HIVE_CLIENT.executeSqlWithoutResultSet(HIVE_SOURCE_DTO, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("create table loader_test_downloader_orc (id int comment 'ID', name string comment '姓名_name') partitioned by (pt string) stored as orc").build();
        HIVE_CLIENT.executeSqlWithoutResultSet(HIVE_SOURCE_DTO, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("insert into loader_test_downloader_orc partition (pt = '2020') values (1, 'loader_test_1')").build();
        HIVE_CLIENT.executeSqlWithoutResultSet(HIVE_SOURCE_DTO, queryDTO);
    }

    @Test
    public void hiveTextDownload () throws Exception {
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().columns(Lists.newArrayList("id", "pt")).tableName("loader_test_downloader_text").build();
        IDownloader downloader = HIVE_CLIENT.getDownloader(HIVE_SOURCE_DTO, queryDTO);
        Assert.assertTrue(CollectionUtils.isNotEmpty(downloader.getMetaInfo()));
        while (!downloader.reachedEnd()){
            Assert.assertNotNull(downloader.readNext());
        }
    }

    @Test
    public void hiveParquetDownload () throws Exception {
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().columns(Lists.newArrayList("id", "pt")).tableName("loader_test_downloader_parquet").build();
        IDownloader downloader = HIVE_CLIENT.getDownloader(HIVE_SOURCE_DTO, queryDTO);
        Assert.assertTrue(CollectionUtils.isNotEmpty(downloader.getMetaInfo()));
        while (!downloader.reachedEnd()){
            Assert.assertNotNull(downloader.readNext());
        }
    }

    @Test
    public void hiveOrcDownload () throws Exception {
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().columns(Lists.newArrayList("id", "pt")).tableName("loader_test_downloader_orc").build();
        IDownloader downloader = HIVE_CLIENT.getDownloader(HIVE_SOURCE_DTO, queryDTO);
        Assert.assertTrue(CollectionUtils.isNotEmpty(downloader.getMetaInfo()));
        while (!downloader.reachedEnd()){
            Assert.assertNotNull(downloader.readNext());
        }
    }

    @Test
    public void hive1TextDownload () throws Exception {
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().columns(Lists.newArrayList("id", "pt")).tableName("loader_test_downloader_text").build();
        IDownloader downloader = HIVE1_CLIENT.getDownloader(HIVE_1_SOURCE_DTO, queryDTO);
        Assert.assertTrue(CollectionUtils.isNotEmpty(downloader.getMetaInfo()));
        while (!downloader.reachedEnd()){
            Assert.assertNotNull(downloader.readNext());
        }
    }

    @Test
    public void hive1ParquetDownload () throws Exception {
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().columns(Lists.newArrayList("id", "pt")).tableName("loader_test_downloader_parquet").build();
        IDownloader downloader = HIVE1_CLIENT.getDownloader(HIVE_1_SOURCE_DTO, queryDTO);
        Assert.assertTrue(CollectionUtils.isNotEmpty(downloader.getMetaInfo()));
        while (!downloader.reachedEnd()){
            Assert.assertNotNull(downloader.readNext());
        }
    }

    @Test
    public void hive1OrcDownload () throws Exception {
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().columns(Lists.newArrayList("id", "pt")).tableName("loader_test_downloader_orc").build();
        IDownloader downloader = HIVE1_CLIENT.getDownloader(HIVE_1_SOURCE_DTO, queryDTO);
        Assert.assertTrue(CollectionUtils.isNotEmpty(downloader.getMetaInfo()));
        while (!downloader.reachedEnd()){
            Assert.assertNotNull(downloader.readNext());
        }
    }

    @Test
    public void sparkTextDownload () throws Exception {
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().columns(Lists.newArrayList("id", "pt")).tableName("loader_test_downloader_text").build();
        IDownloader downloader = SPARK_CLIENT.getDownloader(SPARK_SOURCE_DTO, queryDTO);
        Assert.assertTrue(CollectionUtils.isNotEmpty(downloader.getMetaInfo()));
        while (!downloader.reachedEnd()){
            Assert.assertNotNull(downloader.readNext());
        }
    }

    @Test
    public void sparkParquetDownload () throws Exception {
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().columns(Lists.newArrayList("id", "pt")).tableName("loader_test_downloader_parquet").build();
        IDownloader downloader = SPARK_CLIENT.getDownloader(SPARK_SOURCE_DTO, queryDTO);
        Assert.assertTrue(CollectionUtils.isNotEmpty(downloader.getMetaInfo()));
        while (!downloader.reachedEnd()){
            Assert.assertNotNull(downloader.readNext());
        }
    }

    @Test
    public void sparkOrcDownload () throws Exception {
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().columns(Lists.newArrayList("id", "pt")).tableName("loader_test_downloader_orc").build();
        IDownloader downloader = SPARK_CLIENT.getDownloader(SPARK_SOURCE_DTO, queryDTO);
        Assert.assertTrue(CollectionUtils.isNotEmpty(downloader.getMetaInfo()));
        while (!downloader.reachedEnd()){
            Assert.assertNotNull(downloader.readNext());
        }
    }
}
