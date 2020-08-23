package com.dtstack.dtcenter.common.loader.hive;

import com.dtstack.dtcenter.common.loader.rdbms.AbsRdbmsClient;
import com.dtstack.dtcenter.loader.dto.source.HiveSourceDTO;
import org.junit.Test;

public class HiveClientTest {
    private static AbsRdbmsClient rdbsClient = new HiveClient();

    HiveSourceDTO source = HiveSourceDTO.builder()
            .url("jdbc:hive2://cdh-impala2:10000/ceshis_pri")
            .username("root")
            .password("abc123")
            .build();

    HiveSourceDTO source1 = HiveSourceDTO.builder()
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

    @Test
    public void test_issue() throws Exception {
        // 简单测试代码使用，具体全覆盖使用 core 包下面的
    }

}