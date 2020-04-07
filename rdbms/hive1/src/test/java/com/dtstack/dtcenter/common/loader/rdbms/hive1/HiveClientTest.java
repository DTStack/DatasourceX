package com.dtstack.dtcenter.common.loader.rdbms.hive1;

import com.dtstack.dtcenter.common.exception.DtCenterDefException;
import com.dtstack.dtcenter.common.loader.rdbms.common.AbsRdbmsClient;
import com.dtstack.dtcenter.loader.dto.SourceDTO;
import org.apache.commons.lang3.BooleanUtils;
import org.junit.Test;

public class HiveClientTest {
    private static AbsRdbmsClient rdbsClient = new HiveClient();

    @Test
    public void getConnFactory() throws Exception {
        SourceDTO source = SourceDTO.builder()
                .url("jdbc:hive2://cdh-impala2:10000")
                .username("root")
                .password("abc123")
                .build();
        Boolean isConnected = rdbsClient.testCon(source);
        if (!isConnected) {
            throw new DtCenterDefException("数据源连接异常");
        }
    }

    @Test
    public void testConnection() {
        SourceDTO source = SourceDTO.builder()
                .url("jdbc:hive2://172.16.101.227:10000/yuebai")
                .schema("yuebai")
                .defaultFS("hdfs://ns1")
                .config("{\n" +
                        "    \"dfs.ha.namenodes.ns1\": \"nn1,nn2\",\n" +
                        "    \"dfs.namenode.rpc-address.ns1.nn2\": \"172.16.101.136:9000\",\n" +
                        "    \"dfs.client.failover.proxy.provider.ns1\": \"org.apache.hadoop.hdfs.server.namenode.ha" +
                        ".ConfiguredFailoverProxyProvider\",\n" +
                        "    \"dfs.namenode.rpc-address.ns1.nn1\": \"172.16.100.216:9000\",\n" +
                        "    \"dfs.nameservices\": \"ns1\"\n" +
                        "}")
                .build();
        Boolean isConnected = rdbsClient.testCon(source);
        assert BooleanUtils.isTrue(isConnected);
    }
}