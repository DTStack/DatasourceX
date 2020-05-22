package com.dtstack.dtcenter.common.loader.hdfs;

import com.dtstack.dtcenter.common.exception.DtCenterDefException;
import com.dtstack.dtcenter.common.loader.common.AbsRdbmsClient;
import com.dtstack.dtcenter.loader.dto.SourceDTO;
import org.junit.Test;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 17:17 2020/2/27
 * @Description：HDFS 自测
 */
public class HdfsClientTest {
    private static AbsRdbmsClient rdbsClient = new HdfsClient();

    @Test
    public void getConnFactory() throws Exception {
        SourceDTO source = SourceDTO.builder()
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
        Boolean isConnected = rdbsClient.testCon(source);
        if (!isConnected) {
            throw new DtCenterDefException("数据源连接异常");
        }
    }
}
