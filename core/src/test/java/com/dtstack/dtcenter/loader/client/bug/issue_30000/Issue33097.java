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
 * bug描述：进行从hdfs下载hive表数据的时候，如果表路径下第一个文件是_SUCCESS，会导致
 *        获取该路径下所有文件出错，导致下载失败
 * bug连接：<a>http://redmine.prod.dtstack.cn/issues/33097</>
 *
 * bug解决：修改递归获取hdfs指定文件夹下所有文件的方法逻辑
 *
 * @author ：wangchuan
 * date：Created in 5:13 下午 2020/12/2
 * company: www.dtstack.com
 */
public class Issue33097 {

    private static final String localKerberosPath = Issue33097.class.getResource("/bug/issue_33097").getPath();

    private static HiveSourceDTO hiveSourceDTO = HiveSourceDTO.builder()
            .url("jdbc:hive2://172.16.100.214:10000/default")
            .schema("default")
            .defaultFS("hdfs://ns1")
            .username("admin")
            .config("{\n" +
                    "    \"dfs.ha.namenodes.ns1\": \"nn1,nn2\",\n" +
                    "    \"dfs.namenode.rpc-address.ns1.nn2\": \"172.16.101.227:9000\",\n" +
                    "    \"dfs.client.failover.proxy.provider.ns1\": \"org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider\",\n" +
                    "    \"dfs.namenode.rpc-address.ns1.nn1\": \"172.16.101.196:9000\",\n" +
                    "    \"dfs.nameservices\": \"ns1\"\n" +
                    "}")
            .build();

    private static HdfsSourceDTO hdfsSourceDTO = HdfsSourceDTO.builder()
            .defaultFS("hdfs://ns1")
            .config("{\n" +
                    "    \"dfs.ha.namenodes.ns1\": \"nn1,nn2\",\n" +
                    "    \"dfs.namenode.rpc-address.ns1.nn2\": \"172.16.101.227:9000\",\n" +
                    "    \"dfs.client.failover.proxy.provider.ns1\": \"org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider\",\n" +
                    "    \"dfs.namenode.rpc-address.ns1.nn1\": \"172.16.101.196:9000\",\n" +
                    "    \"dfs.nameservices\": \"ns1\"\n" +
                    "}")
            .build();

    /**
     * 数据准备
     */
    @BeforeClass
    public static void setUp () throws Exception {
        System.setProperty("HADOOP_USER_NAME", "admin");
        IClient client = ClientCache.getClient(DataSourceType.HIVE.getVal());
        IHdfsFile hdfsClient = ClientCache.getHdfs(DataSourceType.HDFS.getVal());
        hdfsClient.checkAndDelete(hdfsSourceDTO, "/tmp/bug_33097");
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("drop table if exists bug_33097").build();
        client.executeSqlWithoutResultSet(hiveSourceDTO, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("create external table bug_33097 (id int, name string) partitioned by (pt string) stored as parquet location '/tmp/bug_33097/'").build();
        client.executeSqlWithoutResultSet(hiveSourceDTO, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("insert into bug_33097 partition (pt='2020_12_02') values (1, 'wangchuan') ").build();
        client.executeSqlWithoutResultSet(hiveSourceDTO, queryDTO);
        // 上传_SUCCESS文件到hdfs
        hdfsClient.uploadLocalFileToHdfs(hdfsSourceDTO, localKerberosPath + "/_SUCCESS", "/tmp/bug_33097/");
    }

    @Test
    public void test_for_issue() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.HIVE.getVal());
        IDownloader download = client.getDownloader(hiveSourceDTO, SqlQueryDTO.builder().tableName("bug_33097").build());
        while (!download.reachedEnd()) {
            System.out.println("---------------------------------");
            System.out.println(download.readNext());
            System.out.println("---------------------------------");
        }
    }
}
