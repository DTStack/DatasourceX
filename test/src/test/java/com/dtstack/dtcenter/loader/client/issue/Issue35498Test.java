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
import com.dtstack.dtcenter.loader.source.DataSourceType;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

/**
 *
 * @author ：wangchuan
 * date：Created in 下午7:23 2021/3/2
 * company: www.dtstack.com
 */
public class Issue35498Test extends BaseTest {

    private static HiveSourceDTO source = HiveSourceDTO.builder()
            .url("jdbc:hive2://kudu3:10000/dev")
            .schema("dev")
            .defaultFS("hdfs://ns1")
            .config("{\n" +
                    "    \"dfs.ha.namenodes.ns1\": \"nn1,nn2\",\n" +
                    "    \"dfs.namenode.rpc-address.ns1.nn2\": \"kudu1:9000\",\n" +
                    "    \"dfs.client.failover.proxy.provider.ns1\": \"org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider\",\n" +
                    "    \"dfs.namenode.rpc-address.ns1.nn1\": \"kudu2:9000\",\n" +
                    "    \"dfs.nameservices\": \"ns1\"\n" +
                    "}")
            .username("admin")
            .build();

    @Test
    public void test () throws Exception{
        System.setProperty("HADOOP_USER_NAME", "admin");
        IClient client = ClientCache.getClient(DataSourceType.HIVE.getPluginName());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("drop table if exists loader_test_textfile").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("create table loader_test_textfile (col1 string , col2 string, col3 string) stored as textfile").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("insert into loader_test_textfile values ('xxx', 'xxx', 'xxx'),('xxx', 'xxx', ''),('', 'xxx', 'xxx'),('xxx', '', ''),('', 'xxx', ''),('', '', 'xxx'),('', '', '')").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
        IDownloader downloader = client.getDownloader(source, SqlQueryDTO.builder().tableName("loader_test_textfile").build());
        while (!downloader.reachedEnd()) {
            List<String> row = (List<String>) downloader.readNext();
            Assert.assertEquals(row.size(), downloader.getMetaInfo().size());
        }
    }
}
