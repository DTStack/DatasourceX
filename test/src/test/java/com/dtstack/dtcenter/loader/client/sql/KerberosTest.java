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

import com.dtstack.dtcenter.loader.client.BaseTest;
import com.dtstack.dtcenter.loader.client.ClientCache;
import com.dtstack.dtcenter.loader.client.IKerberos;
import com.dtstack.dtcenter.loader.kerberos.HadoopConfTool;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import org.apache.commons.collections.MapUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 16:13 2020/9/10
 * @Description：Kerberos 工具测试
 */
public class KerberosTest extends BaseTest {
    IKerberos kerberos = ClientCache.getKerberos(DataSourceType.HIVE.getVal());
    String localParentPath;
    Map<String, Object> kerberosConfig;

    @Before
    public void setUp() throws Exception {
        localParentPath = KerberosTest.class.getResource("/eng-cdh").getPath();
        parseKerberosFromUpload();
    }

    public void parseKerberosFromUpload() throws Exception {
        String zipLos = localParentPath + "/hive-krb.zip";
        String localKerberosPath = localParentPath + "/kerberosFile";
        kerberosConfig = kerberos.parseKerberosFromUpload(zipLos, localKerberosPath);
    }

    @Test
    public void prepareKerberosForConnect() throws Exception {
        Assert.assertEquals("/hive-cdh01.keytab", MapUtils.getString(kerberosConfig, HadoopConfTool.PRINCIPAL_FILE));
        Assert.assertEquals("/krb5.conf", MapUtils.getString(kerberosConfig, HadoopConfTool.KEY_JAVA_SECURITY_KRB5_CONF));
        String localKerberosPath = localParentPath + "/kerberosFile";
        kerberos.prepareKerberosForConnect(kerberosConfig, localKerberosPath);
        Assert.assertEquals(localKerberosPath + "/hive-cdh01.keytab", MapUtils.getString(kerberosConfig, HadoopConfTool.PRINCIPAL_FILE));
        Assert.assertEquals(localKerberosPath + "/krb5.conf", MapUtils.getString(kerberosConfig, HadoopConfTool.KEY_JAVA_SECURITY_KRB5_CONF));
    }

    @Test
    public void getPrincipals() throws Exception {
        String principals = kerberos.getPrincipals("jdbc:hive2://eng-cdh3:10001/default;principal=hive/eng-cdh3@DTSTACK.COM");
        Assert.assertEquals("hive/eng-cdh3@DTSTACK.COM", principals);
    }

    @Test
    public void getPrincipalsFromConfig() throws Exception {
        prepareKerberosForConnect();
        List<String> principals = kerberos.getPrincipals(kerberosConfig);
        Assert.assertNotNull(principals);
    }

}
