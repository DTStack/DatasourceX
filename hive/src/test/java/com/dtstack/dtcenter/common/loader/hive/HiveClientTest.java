package com.dtstack.dtcenter.common.loader.hive;

import com.dtstack.dtcenter.common.loader.common.AbsRdbmsClient;
import com.dtstack.dtcenter.loader.downloader.IDownloader;
import com.dtstack.dtcenter.loader.dto.ColumnMetaDTO;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.source.HiveSourceDTO;
import org.apache.commons.lang3.BooleanUtils;
import org.junit.Test;

import java.util.List;
import java.util.Map;

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

    String sql = "CREATE TABLE `sales`(`id` int COMMENT '', `userid` string COMMENT '', `username` string COMMENT '', `prodid` string COMMENT '', `price` double COMMENT '', `amount` int COMMENT '', `discount` double COMMENT '', `tm` string COMMENT '')\n" +
            "PARTITIONED BY (`pt` string COMMENT '')\n" +
            "ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.orc.OrcSerde'\n" +
            "WITH SERDEPROPERTIES (\n" +
            "  'serialization.format' = '1'\n" +
            ")\n" +
            "STORED AS\n" +
            "  INPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'\n" +
            "  OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'\n" +
            "TBLPROPERTIES (\n" +
            "  'transient_lastDdlTime' = '1586223742'\n" +
            ")";
    @Test
    public void getConnFactory() throws Exception {

        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("nanqi200228").filterPartitionColumns(false).build();
//        List<String> tableList = rdbsClient.getTableList(source, null);
        List<ColumnMetaDTO> columnMetaData = rdbsClient.getColumnMetaData(source, queryDTO);
        System.out.println(columnMetaData.size());
    }

    @Test
    public void testConnection() {
        Boolean isConnected = rdbsClient.testCon(source1);
        assert BooleanUtils.isTrue(isConnected);
    }

    @Test
    public void testDownloader() throws Exception {
        HiveSourceDTO source = HiveSourceDTO.builder()
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
        for (int i = 1; i < 8; i++) {
            IDownloader downloader = rdbsClient.getDownloader(source, SqlQueryDTO.builder().tableName("wangchuan00"+i).build());
            System.out.println("================wangchuan00"+i+"==================");
            System.out.println(downloader.getMetaInfo());
            while (!downloader.reachedEnd()){
                System.out.println(downloader.readNext());
            }
        }
    }

    @Test
    public void testExecuteQuery() throws Exception {
        HiveSourceDTO source = HiveSourceDTO.builder()
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
        List<Map<String, Object>> list = rdbsClient.executeQuery(source, SqlQueryDTO.builder().sql("desc formatted wangchuan005").build());
        for (Map m:list){
            System.out.println(m);
        }

    }

    @Test
    public void getAllDatabase() throws Exception{
        System.out.println(rdbsClient.getAllDatabases(source1,SqlQueryDTO.builder().build()));
    }

    @Test
    public void getCreateSql() throws Exception{
        System.out.println(rdbsClient.getCreateTableSql(source1,SqlQueryDTO.builder().tableName("sales").build()));
    }

    @Test
    public void getAllTables() throws Exception{
        System.out.println(rdbsClient.getTableList(source1,SqlQueryDTO.builder().build()));
    }

    @Test
    public void getPartitionColumn() throws Exception{
        System.out.println(rdbsClient.getPartitionColumn(source1,SqlQueryDTO.builder().tableName("sales").build()));
    }

}