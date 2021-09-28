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
import com.dtstack.dtcenter.loader.client.BaseTest;
import com.dtstack.dtcenter.loader.client.ClientCache;
import com.dtstack.dtcenter.loader.client.IClient;
import com.dtstack.dtcenter.loader.client.IKerberos;
import com.dtstack.dtcenter.loader.dto.ColumnMetaDTO;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.source.ImpalaSourceDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.dtstack.dtcenter.loader.kerberos.HadoopConfTool;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 10:33 2020/9/10
 * @Description：Impala Kerberos 测试
 */
public class ImpalaKerberosTest extends BaseTest {
    private static ImpalaSourceDTO source = ImpalaSourceDTO.builder()
            .url("jdbc:impala://172.16.100.208:21050;AuthMech=1;KrbRealm=DTSTACK.COM;KrbHostFQDN=master;KrbServiceName=impala")
            .username("admin")
            .build();

    @BeforeClass
    public static void beforeClass() {
        // 准备 Kerberos 参数
        Map<String, Object> kerberosConfig = new HashMap<>();
        kerberosConfig.put(HadoopConfTool.PRINCIPAL, "impala/master@DTSTACK.COM");
        kerberosConfig.put(HadoopConfTool.PRINCIPAL_FILE, "/impala.keytab");
        kerberosConfig.put(HadoopConfTool.KEY_JAVA_SECURITY_KRB5_CONF, "/krb5.conf");
        source.setKerberosConfig(kerberosConfig);

        String localKerberosPath = ImpalaKerberosTest.class.getResource("/eng-cdh3").getPath();
        IKerberos kerberos = ClientCache.getKerberos(DataSourceType.IMPALA.getVal());
        kerberos.prepareKerberosForConnect(kerberosConfig, localKerberosPath);

        IClient client = ClientCache.getClient(DataSourceType.IMPALA.getVal());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("drop table if exists nanqi").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("create table nanqi (`class` int comment 'ID', `name` string comment '姓名_name') comment 'table comment'").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("insert into nanqi values (1, 'nanqi')").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
    }

    @Test
    public void getCon() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.IMPALA.getVal());
        Connection con1 = client.getCon(source);
        con1.close();
    }

    @Test
    public void downloader() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.IMPALA.getVal());
        IDownloader downloader = client.getDownloader(source, SqlQueryDTO.builder().sql("select * from nanqi").build());
        while(!downloader.reachedEnd()) {
            System.out.println(downloader.readNext());
        }
    }

    @Test
    public void ex() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.IMPALA.getVal());
        List<Map<String, Object>> list= client.executeQuery(source, SqlQueryDTO.builder().sql("select * from nanqi order by name limit 100 offset 0").build());
        System.out.println(list);
    }

    @Test
    public void testCon() {
        IClient client = ClientCache.getClient(DataSourceType.IMPALA.getVal());
        Boolean isConnected = client.testCon(source);
        if (Boolean.FALSE.equals(isConnected)) {
            throw new DtLoaderException("connection exception");
        }
    }

    @Test
    public void executeQuery() {
        IClient client = ClientCache.getClient(DataSourceType.IMPALA.getVal());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("show databases").build();
        List<Map<String, Object>> mapList = client.executeQuery(source, queryDTO);
        System.out.println(mapList.size());
    }

    @Test
    public void executeSqlWithoutResultSet() {
        IClient client = ClientCache.getClient(DataSourceType.IMPALA.getVal());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("show tables").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
    }

    @Test
    public void getTableList() {
        IClient client = ClientCache.getClient(DataSourceType.IMPALA.getVal());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().build();
        List<String> tableList = client.getTableList(source, queryDTO);
        System.out.println(tableList);
    }

    @Test
    public void getColumnClassInfo() {
        IClient client = ClientCache.getClient(DataSourceType.IMPALA.getVal());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("nanqi").build();
        List<String> columnClassInfo = client.getColumnClassInfo(source, queryDTO);
        Assert.assertEquals("java.lang.Integer", columnClassInfo.get(0));
        Assert.assertEquals("java.lang.String", columnClassInfo.get(1));
    }

    @Test
    public void getColumnMetaData() {
        IClient client = ClientCache.getClient(DataSourceType.IMPALA.getVal());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("nanqi").build();
        List<ColumnMetaDTO> columnMetaData = client.getColumnMetaData(source, queryDTO);
        Assert.assertEquals("int", columnMetaData.get(0).getType());
        Assert.assertEquals("string", columnMetaData.get(1).getType());

    }

    @Test
    public void getTableMetaComment() {
        IClient client = ClientCache.getClient(DataSourceType.IMPALA.getVal());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("nanqi").build();
        String tableMetaComment = client.getTableMetaComment(source, queryDTO);
        Assert.assertTrue(StringUtils.isNotEmpty(tableMetaComment));
    }

    @Test
    public void getPreview() {
        IClient client = ClientCache.getClient(DataSourceType.IMPALA.getVal());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().previewNum(2).tableName("nanqi").build();
        List preview = client.getPreview(source, queryDTO);
        System.out.println(preview);
    }

    @Test
    public void getAllDatabases() {
        IClient client = ClientCache.getClient(DataSourceType.IMPALA.getVal());
        List<String> list = client.getAllDatabases(source, SqlQueryDTO.builder().build());
        Assert.assertTrue(CollectionUtils.isNotEmpty(list));
    }

    @Test
    public void getPartitionColumn() {
        IClient client = ClientCache.getClient(DataSourceType.IMPALA.getVal());
        System.out.println(client.getPartitionColumn(source, SqlQueryDTO.builder().tableName("nanqi").build()));
    }

    @Test
    public void getCreateSql() {
        IClient client = ClientCache.getClient(DataSourceType.IMPALA.getVal());
        String createSql = client.getCreateTableSql(source, SqlQueryDTO.builder().tableName("nanqi").build());
        Assert.assertNotNull(createSql);
    }


    @Test
    public void test() {
        IClient client = ClientCache.getClient(DataSourceType.IMPALA.getVal());
        List<Map<String,Object>> list = client.executeQuery(source, SqlQueryDTO.builder().sql("show tables").build());
        Assert.assertNotNull(list);
    }

    @Test
    public void tes1t() {
        IClient client = ClientCache.getClient(DataSourceType.IMPALA.getVal());
        List<Map<String,Object>> list = client.executeQuery(source, SqlQueryDTO.builder().sql("DESCRIBE extended nanqi").build());
        Assert.assertNotNull(list);
    }
}
