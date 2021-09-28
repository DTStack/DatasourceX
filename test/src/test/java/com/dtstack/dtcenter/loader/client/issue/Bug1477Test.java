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

package com.dtstack.dtcenter.loader.client.issue;

import com.dtstack.dtcenter.loader.IDownloader;
import com.dtstack.dtcenter.loader.client.BaseTest;
import com.dtstack.dtcenter.loader.client.ClientCache;
import com.dtstack.dtcenter.loader.client.IClient;
import com.dtstack.dtcenter.loader.client.IHdfsFile;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.source.HdfsSourceDTO;
import com.dtstack.dtcenter.loader.dto.source.HiveSourceDTO;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import org.apache.commons.collections.CollectionUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * bug内容：当hive表存在但是hive表在hdfs上的路径file is not exist时，调用Client.getDownloader方法会报错 {@link IClient#getDownloader}
 *
 * @author ：wangchuan
 * date：Created in 4:52 下午 2021/2/8
 * company: www.dtstack.com
 */
public class Bug1477Test extends BaseTest {

    /**
     * 构造hive客户端
     */
    private static final IClient HIVE_CLIENT = ClientCache.getClient(DataSourceType.HIVE.getVal());

    /**
     * 构造HDFS_FILE客户端
     */
    private static final IHdfsFile HDFS_FILE_CLIENT = ClientCache.getHdfs(DataSourceType.HDFS.getVal());


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
            .build();

    // 初始化hdfs数据源信息
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
    public static void setUp () {
        System.setProperty("HADOOP_USER_NAME", "admin");
        // text 格式表
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("drop table if exists loader_bug1477_text").build();
        HIVE_CLIENT.executeSqlWithoutResultSet(HIVE_SOURCE_DTO, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("create table loader_bug1477_text (id int) stored as textfile").build();
        HIVE_CLIENT.executeSqlWithoutResultSet(HIVE_SOURCE_DTO, queryDTO);
        // parquet 格式表
        queryDTO = SqlQueryDTO.builder().sql("drop table if exists loader_bug1477_parquet").build();
        HIVE_CLIENT.executeSqlWithoutResultSet(HIVE_SOURCE_DTO, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("create table loader_bug1477_parquet (id int) stored as parquet").build();
        HIVE_CLIENT.executeSqlWithoutResultSet(HIVE_SOURCE_DTO, queryDTO);
        // orc 格式表
        queryDTO = SqlQueryDTO.builder().sql("drop table if exists loader_bug1477_orc").build();
        HIVE_CLIENT.executeSqlWithoutResultSet(HIVE_SOURCE_DTO, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("create table loader_bug1477_orc (id int) stored as orc").build();
        HIVE_CLIENT.executeSqlWithoutResultSet(HIVE_SOURCE_DTO, queryDTO);

        // 删除表根路径
        String textTableLocation = HIVE_CLIENT.getTable(HIVE_SOURCE_DTO, SqlQueryDTO.builder().tableName("loader_bug1477_text").build()).getPath();
        HDFS_FILE_CLIENT.delete(HDFS_SOURCE_DTO, textTableLocation, true);
        String parquetTableLocation = HIVE_CLIENT.getTable(HIVE_SOURCE_DTO, SqlQueryDTO.builder().tableName("loader_bug1477_parquet").build()).getPath();
        HDFS_FILE_CLIENT.delete(HDFS_SOURCE_DTO, parquetTableLocation, true);
        String orcTableLocation = HIVE_CLIENT.getTable(HIVE_SOURCE_DTO, SqlQueryDTO.builder().tableName("loader_bug1477_orc").build()).getPath();
        HDFS_FILE_CLIENT.delete(HDFS_SOURCE_DTO, orcTableLocation, true);
    }

    @Test
    public void test_for_bug_text () throws Exception{
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("loader_bug1477_text").build();
        IDownloader downloader = HIVE_CLIENT.getDownloader(HIVE_SOURCE_DTO, queryDTO);
        Assert.assertTrue(CollectionUtils.isNotEmpty(downloader.getMetaInfo()));
        while (!downloader.reachedEnd()){
            Assert.assertNotNull(downloader.readNext());
        }
    }

    @Test
    public void test_for_bug_parquet () throws Exception {
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("loader_bug1477_parquet").build();
        IDownloader downloader = HIVE_CLIENT.getDownloader(HIVE_SOURCE_DTO, queryDTO);
        Assert.assertTrue(CollectionUtils.isNotEmpty(downloader.getMetaInfo()));
        while (!downloader.reachedEnd()){
            Assert.assertNotNull(downloader.readNext());
        }
    }

    @Test
    public void test_for_bug_orc () throws Exception {
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("loader_bug1477_orc").build();
        IDownloader downloader = HIVE_CLIENT.getDownloader(HIVE_SOURCE_DTO, queryDTO);
        Assert.assertTrue(CollectionUtils.isNotEmpty(downloader.getMetaInfo()));
        while (!downloader.reachedEnd()){
            Assert.assertNotNull(downloader.readNext());
        }
    }
}
