package com.dtstack.dtcenter.loader.client.sql;

import com.alibaba.fastjson.JSONObject;
import com.dtstack.dtcenter.loader.client.ClientCache;
import com.dtstack.dtcenter.loader.client.IHdfsFile;
import com.dtstack.dtcenter.loader.client.IKerberos;
import com.dtstack.dtcenter.loader.dto.HdfsWriterDTO;
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
 * @Date ：Created in 17:44 2020/9/8
 * @Description：hdfs 文件 Kerberos 测试
 */
public class HdfsFileKerberosTest {
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

        String localKerberosPath = HbaseKerberosTest.class.getResource("/eng-cdh").getPath();
        IKerberos kerberos = ClientCache.getKerberos(DataSourceType.HDFS.getVal());
        kerberos.prepareKerberosForConnect(kerberosConfig, localKerberosPath);

        System.setProperty("HADOOP_USER_NAME", "root");
    }

    @Test(expected = DtLoaderException.class)
    public void testHdfsWriter() throws Exception {
        HdfsWriterDTO writerDTO = JSONObject.parseObject("{\"columnsList\":[{\"key\":\"id\",\"part\":false,\"type\":\"int\"},{\"key\":\"name\",\"part\":false,\"type\":\"string\"}],\"fromFileName\":\"/Users/wangbin/Desktop/9c4e5c49-6af2-49f0-863e-293318a3e9a9\",\"fromLineDelimiter\":\",\",\"hdfsDirPath\":\"hdfs://ns1/user/hive/warehouse/dev.db/test_chener_0811\",\"fileFormat\":\"orc\",\"keyList\":[{\"key\":\"id\"},{\"key\":\"name\"}],\"oriCharSet\":\"UTF-8\",\"startLine\":1,\"topLineIsTitle\":true}", HdfsWriterDTO.class);
        IHdfsFile client = ClientCache.getHdfs(DataSourceType.HDFS.getVal());
        int i = client.writeByName(source, writerDTO);
        System.out.println(i);
    }
}
