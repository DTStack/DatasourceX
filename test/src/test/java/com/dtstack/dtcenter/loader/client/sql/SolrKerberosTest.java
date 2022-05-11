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

import com.dtstack.dtcenter.loader.cache.pool.config.PoolConfig;
import com.dtstack.dtcenter.loader.client.BaseTest;
import com.dtstack.dtcenter.loader.client.ClientCache;
import com.dtstack.dtcenter.loader.client.IClient;
import com.dtstack.dtcenter.loader.client.IKerberos;
import com.dtstack.dtcenter.loader.dto.SolrQueryDTO;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.source.SolrSourceDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.dtstack.dtcenter.loader.kerberos.HadoopConfTool;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 11:27 2020/9/10
 * @Description：Kafka Kerberos 认证
 */
@Slf4j
public class SolrKerberosTest extends BaseTest {


    private static final IClient client = ClientCache.getClient(DataSourceType.SOLR.getVal());

    SolrSourceDTO source = SolrSourceDTO.builder()
            .zkHost("worker:2181/solr")
            .poolConfig(new PoolConfig())
            .build();

    @Before
    public void setUp() throws Exception {
        // 准备 Kerberos 参数
        Map<String, Object> kerberosConfig = new HashMap<>();
        kerberosConfig.put(HadoopConfTool.PRINCIPAL, "solr/worker@DTSTACK.COM");
        kerberosConfig.put(HadoopConfTool.PRINCIPAL_FILE, "/solr.keytab");
        kerberosConfig.put(HadoopConfTool.KEY_JAVA_SECURITY_KRB5_CONF, "/krb5.conf");
        source.setKerberosConfig(kerberosConfig);

        String localKerberosPath = SolrKerberosTest.class.getResource("/solr").getPath();
        IKerberos kerberos = ClientCache.getKerberos(DataSourceType.SOLR.getVal());
        kerberos.prepareKerberosForConnect(kerberosConfig, localKerberosPath);
    }

    @Test
    public void testCon() throws Exception {
        Boolean isConnected = client.testCon(source);
        if (Boolean.FALSE.equals(isConnected)) {
            throw new DtLoaderException("connection exception");
        }
        Assert.assertNull(System.getProperty("solr.kerberos.jaas.appname"));
    }

    @Test
    public void getTableList () {
        List<String> databases = client.getTableList(source, SqlQueryDTO.builder().build());
        Assert.assertTrue(CollectionUtils.isNotEmpty(databases));
        Assert.assertNull(System.getProperty("solr.kerberos.jaas.appname"));
    }

    @Test
    public void getPreview() {
        List viewList = client.getPreview(source, SqlQueryDTO.builder().tableName("qianyi_test").previewNum(5).build());
        Assert.assertTrue(CollectionUtils.isNotEmpty(viewList));
        Assert.assertNull(System.getProperty("solr.kerberos.jaas.appname"));
    }

    @Test
    public void getColumnMetaData() {
        List metaData = client.getColumnMetaData(source, SqlQueryDTO.builder().tableName("qianyi_test").build());
        Assert.assertTrue(CollectionUtils.isNotEmpty(metaData));
        Assert.assertNull(System.getProperty("solr.kerberos.jaas.appname"));
    }

    /**
     * 自定义查询
     */
    @Test
    public void executeQuery() {
        SolrQueryDTO solrQueryDTO = new SolrQueryDTO();
        solrQueryDTO.setQuery("name:红豆").setSort(SolrQueryDTO.SortClause.asc("price")).setStart(1).setRows(3).setFields("price","name","id");
        List<Map<String,Object>> metaData = client.executeQuery(source, SqlQueryDTO.builder().tableName("qianyi_test").solrQueryDTO(solrQueryDTO).build());
        Assert.assertTrue(CollectionUtils.isNotEmpty(metaData));
        Assert.assertEquals(3, metaData.size());
        Assert.assertNull(System.getProperty("solr.kerberos.jaas.appname"));
    }

    @Test
    public void clearKerberos() {
        Assert.assertNull(System.getProperty("solr.kerberos.jaas.appname"));
    }

}
