package com.dtstack.dtcenter.loader.client.sql;

import com.dtstack.dtcenter.loader.client.ClientCache;
import com.dtstack.dtcenter.loader.client.IClient;
import com.dtstack.dtcenter.loader.client.IKerberos;
import com.dtstack.dtcenter.loader.dto.source.HdfsSourceDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.dtstack.dtcenter.loader.kerberos.HadoopConfTool;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 17:31 2020/9/8
 * @Description：Hdfs Kerberos 测试
 */
public class HdfsKerberosTest {
    private static HdfsSourceDTO source = HdfsSourceDTO.builder()
            .defaultFS("hdfs://eng-cdh1:8020")
            .build();

    @BeforeClass
    public static void beforeClass() throws Exception {
        // 准备 Kerberos 参数
        Map<String, Object> kerberosConfig = new HashMap<>();
        kerberosConfig.put(HadoopConfTool.PRINCIPAL, "hdfs/eng-cdh1@DTSTACK.COM");
        kerberosConfig.put(HadoopConfTool.PRINCIPAL_FILE, "/hadoop.keytab");
        kerberosConfig.put(HadoopConfTool.KEY_JAVA_SECURITY_KRB5_CONF, "/krb5.conf");
        source.setKerberosConfig(kerberosConfig);

        String localKerberosPath = HdfsKerberosTest.class.getResource("/eng-cdh").getPath();
        IKerberos kerberos = ClientCache.getKerberos(DataSourceType.HDFS.getVal());
        kerberos.prepareKerberosForConnect(kerberosConfig, localKerberosPath);
        System.setProperty("HADOOP_USER_NAME", "root");
    }

    @Test
    public void testCon() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.HDFS.getVal());
        Boolean isConnected = client.testCon(source);
        if (Boolean.FALSE.equals(isConnected)) {
            throw new DtLoaderException("连接异常");
        }
    }
}
