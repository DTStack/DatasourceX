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
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * bug描述：进行从hdfs下载hive表数据的时候，表存储格式parquet，如果表数据文件中有的字段不存在，会导致
 *        下载数据时发生数据越界异常
 * bug连接：<a>http://redmine.prod.dtstack.cn/issues/33045</>
 *
 * bug解决：读取parquet的适合，读取每一行的适合都要度获取该行的字段
 *
 * @author ：wangchuan
 * date：Created in 3:13 下午 2020/12/7
 * company: www.dtstack.com
 */
public class Issue33045Test extends BaseTest {

    private static final String localKerberosPath = Issue33045Test.class.getResource("/bug/issue_33045").getPath();

    private static HiveSourceDTO hiveSourceDTO = HiveSourceDTO.builder()
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

    private static HdfsSourceDTO hdfsSourceDTO = HdfsSourceDTO.builder()
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
    public static void setUp () throws Exception {
        System.setProperty("HADOOP_USER_NAME", "admin");
        IHdfsFile hdfsClient = ClientCache.getHdfs(DataSourceType.HDFS.getVal());
        hdfsClient.checkAndDelete(hdfsSourceDTO, "/tmp/bug_33045");
        IClient client = ClientCache.getClient(DataSourceType.HIVE.getVal());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("drop table if exists bug_33045").build();
        client.executeSqlWithoutResultSet(hiveSourceDTO, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("create external table bug_33045 (id int, name string) stored as parquet location '/tmp/bug_33045/'").build();
        client.executeSqlWithoutResultSet(hiveSourceDTO, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("insert into bug_33045 values (1, 'wangchuan'), (2, 'wangchuan2'), (3, null)").build();
        client.executeSqlWithoutResultSet(hiveSourceDTO, queryDTO);
        // 上传z.parquet文件到hdfs
        hdfsClient.uploadLocalFileToHdfs(hdfsSourceDTO, localKerberosPath + "/z.parquet", "/tmp/bug_33045/");
    }

    @Test
    public void test_for_issue() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.HIVE.getVal());
        IDownloader download = client.getDownloader(hiveSourceDTO, SqlQueryDTO.builder().tableName("bug_33045").build());
        while (!download.reachedEnd()) {
            System.out.println("---------------------------------");
            System.out.println(download.readNext());
            System.out.println("---------------------------------");
        }
    }
}
