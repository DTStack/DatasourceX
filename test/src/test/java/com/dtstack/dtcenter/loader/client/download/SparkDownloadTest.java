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

package com.dtstack.dtcenter.loader.client.download;

import com.dtstack.dtcenter.loader.IDownloader;
import com.dtstack.dtcenter.loader.cache.pool.config.PoolConfig;
import com.dtstack.dtcenter.loader.client.BaseTest;
import com.dtstack.dtcenter.loader.client.ClientCache;
import com.dtstack.dtcenter.loader.client.IClient;
import com.dtstack.dtcenter.loader.client.IHdfsFile;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.source.HdfsSourceDTO;
import com.dtstack.dtcenter.loader.dto.source.SparkSourceDTO;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * spark download 测试
 *
 * @author ：wangchuan
 * date：Created in 下午2:23 2021/7/26
 * company: www.dtstack.com
 */
@Slf4j
public class SparkDownloadTest extends BaseTest {

    /**
     * 构造 hive 客户端
     */
    private static final IClient HIVE_CLIENT = ClientCache.getClient(DataSourceType.SparkThrift2_1.getVal());

    /**
     * 构造 HDFS 客户端
     */
    private static final IHdfsFile HDFS_FILE_CLIENT = ClientCache.getHdfs(DataSourceType.HDFS.getVal());

    /**
     * 本地文件路径
     */
    private static final String FILE_PATH = SparkDownloadTest.class.getResource("/download").getPath();

    /**
     * 构建 HIVE 数据源信息
     */
    private static final SparkSourceDTO HIVE_SOURCE_DTO = SparkSourceDTO.builder()
            .url("jdbc:hive2://172.16.100.214:10004/default")
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
     * 构建 HDFS 数据源信息
     */
    private static final HdfsSourceDTO HDFS_SOURCE_DTO = HdfsSourceDTO.builder()
            .defaultFS("hdfs://ns1")
            .config("{\n" +
                    "    \"dfs.ha.namenodes.ns1\": \"nn1,nn2\",\n" +
                    "    \"dfs.namenode.rpc-address.ns1.nn2\": \"172.16.101.227:9000\",\n" +
                    "    \"dfs.client.failover.proxy.provider.ns1\": \"org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider\",\n" +
                    "    \"dfs.namenode.rpc-address.ns1.nn1\": \"172.16.101.196:9000\",\n" +
                    "    \"dfs.nameservices\": \"ns1\"\n" +
                    "}")
            .build();

    /**
     * 数据准备
     */
    @BeforeClass
    public static void beforeClass() {
        System.setProperty("HADOOP_USER_NAME", "admin");
        /*-------------------------------------textFile--------------------------------------------*/
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("drop table if exists loader_download_text_part").build();
        HIVE_CLIENT.executeSqlWithoutResultSet(HIVE_SOURCE_DTO, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("create table loader_download_text_part (id int, name string) partitioned by (year string, month string, day string) row format delimited fields terminated by ',' stored as textfile").build();
        HIVE_CLIENT.executeSqlWithoutResultSet(HIVE_SOURCE_DTO, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("insert into loader_download_text_part partition (year = '2021', month = '07', day = '26') values (1, 'loader_test_01')").build();
        HIVE_CLIENT.executeSqlWithoutResultSet(HIVE_SOURCE_DTO, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("insert into loader_download_text_part partition (year = '2021', month = '07', day = '27') values (1, 'loader_test_02')").build();
        HIVE_CLIENT.executeSqlWithoutResultSet(HIVE_SOURCE_DTO, queryDTO);

        /*-------------------------------------parquet--------------------------------------------*/
        queryDTO = SqlQueryDTO.builder().sql("drop table if exists loader_download_parquet_part").build();
        HIVE_CLIENT.executeSqlWithoutResultSet(HIVE_SOURCE_DTO, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("create table loader_download_parquet_part (id int, name string) partitioned by (year string, month string, day string) stored as parquet").build();
        HIVE_CLIENT.executeSqlWithoutResultSet(HIVE_SOURCE_DTO, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("insert into loader_download_parquet_part partition (year = '2021', month = '07', day = '26') values (1, 'loader_test_01')").build();
        HIVE_CLIENT.executeSqlWithoutResultSet(HIVE_SOURCE_DTO, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("insert into loader_download_parquet_part partition (year = '2021', month = '07', day = '27') values (1, 'loader_test_02')").build();
        HIVE_CLIENT.executeSqlWithoutResultSet(HIVE_SOURCE_DTO, queryDTO);

        /*--------------------------------------orc----------------------------------------------*/
        queryDTO = SqlQueryDTO.builder().sql("drop table if exists loader_download_orc_part").build();
        HIVE_CLIENT.executeSqlWithoutResultSet(HIVE_SOURCE_DTO, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("create table loader_download_orc_part (id int, name string) partitioned by (year string, month string, day string) stored as orc").build();
        HIVE_CLIENT.executeSqlWithoutResultSet(HIVE_SOURCE_DTO, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("insert into loader_download_orc_part partition (year = '2021', month = '07', day = '26') values (1, 'loader_test_01')").build();
        HIVE_CLIENT.executeSqlWithoutResultSet(HIVE_SOURCE_DTO, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("insert into loader_download_orc_part partition (year = '2021', month = '07', day = '27') values (1, 'loader_test_02')").build();
        HIVE_CLIENT.executeSqlWithoutResultSet(HIVE_SOURCE_DTO, queryDTO);

        // 手动创建分区路径，上传数据到该分区，不进行数据关联分区操作
        String textTablePartPath = HIVE_CLIENT.getTable(HIVE_SOURCE_DTO, SqlQueryDTO.builder().tableName("loader_download_text_part").build()).getPath() + "/year=2021/month=07/day=28";
        String parquetTablePartPath = HIVE_CLIENT.getTable(HIVE_SOURCE_DTO, SqlQueryDTO.builder().tableName("loader_download_parquet_part").build()).getPath() + "/year=2021/month=07/day=28";
        String orcTablePartPath = HIVE_CLIENT.getTable(HIVE_SOURCE_DTO, SqlQueryDTO.builder().tableName("loader_download_orc_part").build()).getPath() + "/year=2021/month=07/day=28";

        try {
            HDFS_FILE_CLIENT.delete(HDFS_SOURCE_DTO, textTablePartPath, true);
            HDFS_FILE_CLIENT.delete(HDFS_SOURCE_DTO, parquetTablePartPath, true);
            HDFS_FILE_CLIENT.delete(HDFS_SOURCE_DTO, orcTablePartPath, true);
        } catch (Exception e) {
            // do nothing
        }

        // 手动创建分区路径
        try {
            HDFS_FILE_CLIENT.createDir(HDFS_SOURCE_DTO, textTablePartPath, null);
            HDFS_FILE_CLIENT.createDir(HDFS_SOURCE_DTO, parquetTablePartPath, null);
            HDFS_FILE_CLIENT.createDir(HDFS_SOURCE_DTO, orcTablePartPath, null);
        } catch (Exception e) {
            // do nothing
        }

        // 上传数据到指定分区路径
        try {
            HDFS_FILE_CLIENT.uploadLocalFileToHdfs(HDFS_SOURCE_DTO, FILE_PATH + "/text", textTablePartPath);
            HDFS_FILE_CLIENT.uploadLocalFileToHdfs(HDFS_SOURCE_DTO, FILE_PATH + "/parquet", parquetTablePartPath);
            HDFS_FILE_CLIENT.uploadLocalFileToHdfs(HDFS_SOURCE_DTO, FILE_PATH + "/orc", orcTablePartPath);
        } catch (Exception e) {
            // do nothing
        }

    }

    @Test
    public void getDownloaderForText() throws Exception {
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("loader_download_text_part").build();
        IDownloader downloaderBefore = HIVE_CLIENT.getDownloader(HIVE_SOURCE_DTO, queryDTO);
        Assert.assertTrue(CollectionUtils.isNotEmpty(downloaderBefore.getMetaInfo()));
        int recordNumBefore = 0;
        while (!downloaderBefore.reachedEnd()) {
            recordNumBefore++;
            Assert.assertNotNull(downloaderBefore.readNext());
        }
        Assert.assertEquals(2, recordNumBefore);
        // 添加分区数据关联
        HIVE_CLIENT.executeQuery(HIVE_SOURCE_DTO, SqlQueryDTO.builder().sql("alter table loader_download_text_part add partition (year = '2021', month = '07', day = '28')").build());
        IDownloader downloaderAfter = HIVE_CLIENT.getDownloader(HIVE_SOURCE_DTO, queryDTO);
        Assert.assertTrue(CollectionUtils.isNotEmpty(downloaderAfter.getMetaInfo()));
        int recordNumAfter = 0;
        while (!downloaderAfter.reachedEnd()) {
            recordNumAfter++;
            Assert.assertNotNull(downloaderAfter.readNext());
        }
        // 添加关联后数据条数为 3 条
        Assert.assertEquals(3, recordNumAfter);
    }

    @Test
    public void getDownloaderForParquet() throws Exception {
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("loader_download_parquet_part").build();
        IDownloader downloaderBefore = HIVE_CLIENT.getDownloader(HIVE_SOURCE_DTO, queryDTO);
        Assert.assertTrue(CollectionUtils.isNotEmpty(downloaderBefore.getMetaInfo()));
        int recordNumBefore = 0;
        while (!downloaderBefore.reachedEnd()) {
            recordNumBefore++;
            Assert.assertNotNull(downloaderBefore.readNext());
        }
        Assert.assertEquals(2, recordNumBefore);
        // 添加分区数据关联
        HIVE_CLIENT.executeQuery(HIVE_SOURCE_DTO, SqlQueryDTO.builder().sql("alter table loader_download_parquet_part add partition (year = '2021', month = '07', day = '28')").build());
        IDownloader downloaderAfter = HIVE_CLIENT.getDownloader(HIVE_SOURCE_DTO, queryDTO);
        Assert.assertTrue(CollectionUtils.isNotEmpty(downloaderAfter.getMetaInfo()));
        int recordNumAfter = 0;
        while (!downloaderAfter.reachedEnd()) {
            recordNumAfter++;
            Assert.assertNotNull(downloaderAfter.readNext());
        }
        // 添加关联后数据条数为 3 条
        Assert.assertEquals(3, recordNumAfter);
    }

    @Test
    public void getDownloaderForOrc() throws Exception {
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("loader_download_orc_part").build();
        IDownloader downloaderBefore = HIVE_CLIENT.getDownloader(HIVE_SOURCE_DTO, queryDTO);
        Assert.assertTrue(CollectionUtils.isNotEmpty(downloaderBefore.getMetaInfo()));
        int recordNumBefore = 0;
        while (!downloaderBefore.reachedEnd()) {
            recordNumBefore++;
            Assert.assertNotNull(downloaderBefore.readNext());
        }
        Assert.assertEquals(2, recordNumBefore);
        // 添加分区数据关联
        HIVE_CLIENT.executeQuery(HIVE_SOURCE_DTO, SqlQueryDTO.builder().sql("alter table loader_download_orc_part add partition (year = '2021', month = '07', day = '28')").build());
        IDownloader downloaderAfter = HIVE_CLIENT.getDownloader(HIVE_SOURCE_DTO, queryDTO);
        Assert.assertTrue(CollectionUtils.isNotEmpty(downloaderAfter.getMetaInfo()));
        int recordNumAfter = 0;
        while (!downloaderAfter.reachedEnd()) {
            recordNumAfter++;
            Assert.assertNotNull(downloaderAfter.readNext());
        }
        // 添加关联后数据条数为 3 条
        Assert.assertEquals(3, recordNumAfter);
    }
}
