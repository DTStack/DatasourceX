package com.dtstack.dtcenter.loader.client.sql;

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
public class KerberosTest {
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
