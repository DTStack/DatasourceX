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
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.source.HiveSourceDTO;
import com.dtstack.dtcenter.loader.dto.source.ImpalaSourceDTO;
import com.dtstack.dtcenter.loader.dto.source.SparkSourceDTO;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Objects;

/**
 * bug描述：sparkSql、hiveSql调用getDownloader查询impala表报错
 *
 * bug连接：<a>http://zenpms.dtstack.cn/zentao/bug-view-35789.html</>
 *
 * @author ：wangchuan
 * date：Created in 下午5:18 2021/3/24
 * company: www.dtstack.com
 */
public class Issue35789Test extends BaseTest {

    private static final ImpalaSourceDTO IMPALA_SOURCE_DTO = ImpalaSourceDTO.builder()
            .url("jdbc:impala://172.16.101.17:21050/default")
            .build();

    private static final SparkSourceDTO SPARK_SOURCE_DTO = SparkSourceDTO.builder()
            .url("jdbc:hive2://172.16.100.214:10004/default")
            .schema("default")
            .defaultFS("hdfs://ns1")
            .config("{\n" +
                    "    \"dfs.ha.namenodes.ns1\": \"nn1,nn2\",\n" +
                    "    \"dfs.namenode.rpc-address.ns1.nn2\": \"172.16.101.227:9000\",\n" +
                    "    \"dfs.client.failover.proxy.provider.ns1\": \"org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider\",\n" +
                    "    \"dfs.namenode.rpc-address.ns1.nn1\": \"172.16.101.196:9000\",\n" +
                    "    \"dfs.nameservices\": \"ns1\"\n" +
                    "}")
            .username("admin")
            .build();

    private static final HiveSourceDTO HIVE_SOURCE_DTO = HiveSourceDTO.builder()
            .url("jdbc:hive2://172.16.100.214:10000/default")
            .schema("default")
            .defaultFS("hdfs://ns1")
            .config("{\n" +
                    "    \"dfs.ha.namenodes.ns1\": \"nn1,nn2\",\n" +
                    "    \"dfs.namenode.rpc-address.ns1.nn2\": \"172.16.101.227:9000\",\n" +
                    "    \"dfs.client.failover.proxy.provider.ns1\": \"org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider\",\n" +
                    "    \"dfs.namenode.rpc-address.ns1.nn1\": \"172.16.101.196:9000\",\n" +
                    "    \"dfs.nameservices\": \"ns1\"\n" +
                    "}")
            .username("admin")
            .build();

    @BeforeClass
    public static void setUp() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.IMPALA.getVal());
        /*-----------------------创建text格式表-----------------------------*/
        String dropTextSql = "drop table if exists loader_test_textfile";
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql(dropTextSql).build();
        client.executeSqlWithoutResultSet(IMPALA_SOURCE_DTO, queryDTO);
        String createTextSql = "create table loader_test_textfile (id int,name string) partitioned by (pt string) stored as textfile";
        queryDTO = SqlQueryDTO.builder().sql(createTextSql).build();
        client.executeSqlWithoutResultSet(IMPALA_SOURCE_DTO, queryDTO);
        String insertTextSql = "insert into loader_test_textfile partition(pt = '2021') values (1,'wangchuan')";
        queryDTO = SqlQueryDTO.builder().sql(insertTextSql).build();
        client.executeSqlWithoutResultSet(IMPALA_SOURCE_DTO, queryDTO);
        /*-----------------------创建parquet格式表-----------------------------*/
        String dropParquetSql = "drop table if exists loader_test_parquet";
        queryDTO = SqlQueryDTO.builder().sql(dropParquetSql).build();
        client.executeSqlWithoutResultSet(IMPALA_SOURCE_DTO, queryDTO);
        String createParquetSql = "create table loader_test_parquet (id int,name string) partitioned by (pt string) stored as parquet";
        queryDTO = SqlQueryDTO.builder().sql(createParquetSql).build();
        client.executeSqlWithoutResultSet(IMPALA_SOURCE_DTO, queryDTO);
        String insertParquetSql = "insert into loader_test_parquet partition(pt = '2021') values (1,'wangchuan')";
        queryDTO = SqlQueryDTO.builder().sql(insertParquetSql).build();
        client.executeSqlWithoutResultSet(IMPALA_SOURCE_DTO, queryDTO);
        // PS：impala 3.1之前不支持orc存储格式切orc读取逻辑不同于text、parquet，此处不做处理
    }

    @Test
    public void test_for_issue_spark_text() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.Spark.getVal());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("loader_test_textfile").build();
        IDownloader downloader = client.getDownloader(SPARK_SOURCE_DTO, queryDTO);
        System.out.println(downloader.getMetaInfo());
        while (!downloader.reachedEnd()) {
            Assert.assertTrue(Objects.nonNull(downloader.readNext()));
        }
    }

    @Test
    public void test_for_issue_spark_parquet() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.Spark.getVal());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("loader_test_parquet").build();
        IDownloader downloader = client.getDownloader(SPARK_SOURCE_DTO, queryDTO);
        System.out.println(downloader.getMetaInfo());
        while (!downloader.reachedEnd()) {
            Assert.assertTrue(Objects.nonNull(downloader.readNext()));
        }
    }

    @Test
    public void test_for_issue_hive_text() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.HIVE.getVal());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("loader_test_textfile").build();
        IDownloader downloader = client.getDownloader(HIVE_SOURCE_DTO, queryDTO);
        System.out.println(downloader.getMetaInfo());
        while (!downloader.reachedEnd()) {
            Assert.assertTrue(Objects.nonNull(downloader.readNext()));
        }
    }

    @Test
    public void test_for_issue_hive_parquet() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.HIVE.getVal());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("loader_test_parquet").build();
        IDownloader downloader = client.getDownloader(HIVE_SOURCE_DTO, queryDTO);
        System.out.println(downloader.getMetaInfo());
        while (!downloader.reachedEnd()) {
            Assert.assertTrue(Objects.nonNull(downloader.readNext()));
        }
    }
}
