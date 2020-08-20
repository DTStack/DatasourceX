package com.dtstack.dtcenter.common.loader.hbase;

import com.dtstack.dtcenter.common.loader.common.AbsRdbmsClient;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.source.HbaseSourceDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 21:42 2020/2/27
 * @Description：Hbase 客户端测试
 */
@Slf4j
public class HbaseClientTest {
    private static AbsRdbmsClient rdbsClient = new HbaseClient();
    private String conf = "{\"hbase.zookeeper.quorum\":\"172.16.10.104:2181,172.16.10.224:2181,172.16.10.252:2181\"," +
            "\"zookeeper.znode.parent\":\"/hbase\"}";
    private HbaseSourceDTO source = HbaseSourceDTO.builder().kerberosConfig(null)
            .config(conf).build();

    private HbaseSourceDTO source2 = HbaseSourceDTO.builder().kerberosConfig(null).url("172.16.10.104:2181,172.16.10" +
            ".224:2181,172.16.10.252:2181")
            .path("/hbase").build();

    @Test
    public void testCon() throws Exception {
        Boolean isConnected = rdbsClient.testCon(source);
        if (!isConnected) {
            throw new DtLoaderException("数据源连接异常");
        }
    }

    @Test
    public void getTableList() throws Exception {
        List tableList = rdbsClient.getTableList(source, null);
        System.out.println(tableList);
    }

    @Test
    public void getColumnMetaData() throws Exception {
        SqlQueryDTO sqlQueryDTO = SqlQueryDTO.builder().tableName("table2").build();
        List columnMetaData = rdbsClient.getColumnMetaData(source, sqlQueryDTO);
        System.out.println(columnMetaData);
    }

    @Test
    public void testKerberos() {
        System.setProperty("java.security.krb5.conf", "/etc/krb5.conf");

        Configuration configuration = HBaseConfiguration.create();

        System.out.println(configuration.get("hbase.rootdir"));

        configuration.set("hadoop.security.authentication", "Kerberos");

        System.setProperty("sun.security.krb5.debug", "true");

        UserGroupInformation.setConfiguration(configuration);

        try {

            UserGroupInformation.loginUserFromKeytab("sparktest@DTSTACK.COM", "/Users/jialongyan/IdeaProjects/dtStack/dt-center-streamapp/kerberosConf/STREAM_0/USER_5/PROJECT_11/sparktest.keytab");

            configuration.set("hbase.zookeeper.quorum", "172.16.10.59,172.16.10.248,172.16.10.241:2181");
            configuration.set("hbase.security.authentication", "kerberos");
            configuration.set("hadoop.security.authentication" , "Kerberos" );
            configuration.set("hbase.master.kerberos.principal", "hbase/_HOST@DTSTACK.COM");
            configuration.set("hbase.regionserver.kerberos.principal", "hbase/_HOST@DTSTACK.COM");
            // 设置默认信息
            configuration.set("hbase.rpc.timeout", "6000");
            configuration.set("ipc.socket.timeout", "2000");
            configuration.set("hbase.client.retries.number", "3");
            configuration.set("hbase.client.pause", "100");
            configuration.set("zookeeper.recovery.retry", "3");
            Connection connection = ConnectionFactory.createConnection(configuration);

            Assert.assertNotNull(connection.getAdmin().getClusterStatus());

            for (TableName tableName : connection.getAdmin().listTableNames()) {
                System.out.println(tableName);
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }
}
