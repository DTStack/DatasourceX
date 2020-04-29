package com.dtstack.dtcenter.common.loader.rdbms.hive;

import com.dtstack.dtcenter.common.loader.rdbms.common.AbsRdbmsClient;
import com.dtstack.dtcenter.loader.dto.ColumnMetaDTO;
import com.dtstack.dtcenter.loader.dto.SourceDTO;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import org.apache.commons.lang3.BooleanUtils;
import org.junit.Test;

import java.util.List;

public class HiveClientTest {
    private static AbsRdbmsClient rdbsClient = new HiveClient();

    @Test
    public void getConnFactory() throws Exception {
        SourceDTO source = SourceDTO.builder()
                .url("jdbc:hive2://cdh-impala2:10000/ceshis_pri")
                .username("root")
                .password("abc123")
                .build();
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("nanqi200228").filterPartitionColumns(false).build();
//        List<String> tableList = rdbsClient.getTableList(source, null);
        List<ColumnMetaDTO> columnMetaData = rdbsClient.getColumnMetaData(source, queryDTO);
        System.out.println(columnMetaData.size());
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