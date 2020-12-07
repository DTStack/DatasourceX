package com.dtstack.dtcenter.loader.client.sql;

import com.dtstack.dtcenter.loader.IDownloader;
import com.dtstack.dtcenter.loader.client.ClientCache;
import com.dtstack.dtcenter.loader.client.IClient;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.source.SparkSourceDTO;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * -
 *
 * @author ：wangchuan
 * date：Created in 9:39 下午 2020/12/1
 * company: www.dtstack.com
 */
public class KerberosSparkTest {

    private static SparkSourceDTO source = SparkSourceDTO.builder()
            .url("jdbc:hive2://eng-cdh3:10000/default;principal=hive/eng-cdh3@DTSTACK.COM")
            .schema("default")
            .defaultFS("hdfs://eng-cdh1:8020")
            .config("{\"defaultFs\":\"hdfs://eng-cdh1:8020\"}")
            .build();

    @BeforeClass
    public static void beforeClass() throws Exception {
        System.setProperty("HADOOP_USER_NAME", "root");
        // 准备 Kerberos 参数
        Map<String, Object> kerberosConfig = new HashMap<>();
        String localKerberosPath = KerberosSparkTest.class.getResource("/eng-cdh").getPath();
        kerberosConfig.put("principal", "hive/eng-cdh3@DTSTACK.COM");
        kerberosConfig.put("principalFile", localKerberosPath + "/hive-cdh03.keytab");
        kerberosConfig.put("java.security.krb5.conf", localKerberosPath + "/krb5.conf");
        source.setKerberosConfig(kerberosConfig);
    }

    @Test
    public void testConn() throws Exception{
        IClient client = ClientCache.getClient(DataSourceType.Spark.getVal());
        System.out.println(client.testCon(source));
    }

    @Test
    public void getDownload() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.Spark.getVal());
        IDownloader downloader = client.getDownloader(source, SqlQueryDTO.builder().tableName("test_result").build());
        System.out.println(downloader.getMetaInfo());
        while (!downloader.reachedEnd()) {
            System.out.println("==================");
            System.out.println(downloader.readNext());
            System.out.println("==================");
        }
    }
}
