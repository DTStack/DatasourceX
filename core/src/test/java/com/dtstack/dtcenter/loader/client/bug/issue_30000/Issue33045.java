package com.dtstack.dtcenter.loader.client.bug.issue_30000;

import com.dtstack.dtcenter.loader.IDownloader;
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
public class Issue33045 {

    private static final String localKerberosPath = Issue33097.class.getResource("/bug/issue_33045").getPath();

    private static HiveSourceDTO hiveSourceDTO = HiveSourceDTO.builder()
            .url("jdbc:hive2://kudu1:10000/dev")
            .schema("dev")
            .defaultFS("hdfs://ns1")
            .username("admin")
            .config("{\n" +
                    "    \"dfs.ha.namenodes.ns1\": \"nn1,nn2\",\n" +
                    "    \"dfs.namenode.rpc-address.ns1.nn2\": \"kudu2:9000\",\n" +
                    "    \"dfs.client.failover.proxy.provider.ns1\": \"org.apache.hadoop.hdfs.server.namenode.ha" +
                    ".ConfiguredFailoverProxyProvider\",\n" +
                    "    \"dfs.namenode.rpc-address.ns1.nn1\": \"kudu1:9000\",\n" +
                    "    \"dfs.nameservices\": \"ns1\"\n" +
                    "}")
            .build();

    private static HdfsSourceDTO hdfsSourceDTO = HdfsSourceDTO.builder()
            .defaultFS("hdfs://ns1")
            .config("{\n" +
                    "    \"dfs.ha.namenodes.ns1\": \"nn1,nn2\",\n" +
                    "    \"dfs.namenode.rpc-address.ns1.nn2\": \"kudu2:9000\",\n" +
                    "    \"dfs.client.failover.proxy.provider.ns1\": \"org.apache.hadoop.hdfs.server.namenode.ha" +
                    ".ConfiguredFailoverProxyProvider\",\n" +
                    "    \"dfs.namenode.rpc-address.ns1.nn1\": \"kudu1:9000\",\n" +
                    "    \"dfs.nameservices\": \"ns1\"\n" +
                    "}")
            .build();

    /**
     * 数据准备
     */
    @BeforeClass
    public static void setUp () throws Exception {
        System.setProperty("HADOOP_USER_NAME", "root");
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
