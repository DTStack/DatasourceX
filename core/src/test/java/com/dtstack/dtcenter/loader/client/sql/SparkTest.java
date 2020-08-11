package com.dtstack.dtcenter.loader.client.sql;

import com.dtstack.dtcenter.common.exception.DtCenterDefException;
import com.dtstack.dtcenter.loader.IDownloader;
import com.dtstack.dtcenter.loader.client.AbsClientCache;
import com.dtstack.dtcenter.loader.client.IClient;
import com.dtstack.dtcenter.loader.dto.ColumnMetaDTO;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.source.HiveSourceDTO;
import com.dtstack.dtcenter.loader.enums.ClientType;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import org.junit.Test;

import java.sql.Connection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 00:13 2020/2/29
 * @Description：Spark 测试
 */
public class SparkTest {
    private static final AbsClientCache clientCache = ClientType.DATA_SOURCE_CLIENT.getClientCache();

    HiveSourceDTO source = HiveSourceDTO.builder()
            .url("jdbc:hive2://172.16.8.107:10000/default")
            .schema("default")
            .defaultFS("hdfs://ns1")
            .config("{\n" +
                    "    \"dfs.ha.namenodes.ns1\": \"nn1,nn2\",\n" +
                    "    \"dfs.namenode.rpc-address.ns1.nn2\": \"kudu2:9000\",\n" +
                    "    \"dfs.client.failover.proxy.provider.ns1\": \"org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider\",\n" +
                    "    \"dfs.namenode.rpc-address.ns1.nn1\": \"kudu1:9000\",\n" +
                    "    \"dfs.nameservices\": \"ns1\"\n" +
                    "}")
            .username("admin")
            .build();

    @Test
    public void getCon() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.Spark.getPluginName());
        Connection con = client.getCon(source);
        con.createStatement().close();
        con.close();
    }

    @Test
    public void testCon() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.Spark.getPluginName());
        Boolean isConnected = client.testCon(source);
        if (Boolean.FALSE.equals(isConnected)) {
            throw new DtCenterDefException("连接异常");
        }
    }

    @Test
    public void executeQuery() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.Spark.getPluginName());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("show tables").build();
        List<Map<String, Object>> mapList = client.executeQuery(source, queryDTO);
        System.out.println(mapList.size());
    }

    @Test
    public void executeSqlWithoutResultSet() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.Spark.getPluginName());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("show tables").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
    }

    @Test
    public void getTableList() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.Spark.getPluginName());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().build();
        List<String> tableList = client.getTableList(source, queryDTO);
        System.out.println(tableList);
    }

    @Test
    public void getColumnClassInfo() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.Spark.getPluginName());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("nanqi_partition").build();
        List<String> columnClassInfo = client.getColumnClassInfo(source, queryDTO);
        System.out.println(columnClassInfo.size());
    }

    @Test
    public void getColumnMetaData() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.Spark.getPluginName());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("nanqi_partition").build();
        List<ColumnMetaDTO> columnMetaData = client.getColumnMetaData(source, queryDTO);
        System.out.println(columnMetaData.size());
    }

    @Test
    public void getTableMetaComment() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.Spark.getPluginName());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("nanqi_partition").build();
        String metaComment = client.getTableMetaComment(source, queryDTO);
        System.out.println(metaComment);
    }

    @Test
    public void getDownloader() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.Spark.getPluginName());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("nanqi_partition").build();
        IDownloader downloader = client.getDownloader(source, queryDTO);
        System.out.println(downloader.getMetaInfo());
        while (!downloader.reachedEnd()){
            System.out.println(downloader.readNext());
        }
    }

    @Test
    public void getPreview() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.Spark.getPluginName());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("nanqi_partition").build();
        List preview = client.getPreview(source, queryDTO);
        System.out.println(preview);
    }

    @Test
    public void getPartitionColumn() throws Exception{
        IClient client = clientCache.getClient(DataSourceType.Spark.getPluginName());
        List<ColumnMetaDTO> data = client.getColumnMetaData(source, SqlQueryDTO.builder().tableName("nanqi_partition").build());
        data.forEach(x-> System.out.println(x.getKey()+"=="+x.getPart()));
    }

    @Test
    public void getPreview2() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.Spark.getPluginName());
        HashMap<String, String> map = new HashMap<>();
        map.put("pt1", "1");
        map.put("pt2", "2");
        List list = client.getPreview(source, SqlQueryDTO.builder().tableName("nanqi_partition").partitionColumns(map).build());
        System.out.println(list);
    }

    @Test
    public void query() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.Spark.getPluginName());
        List list = client.executeQuery(source, SqlQueryDTO.builder().sql("select * from nanqi_partition").build());
        System.out.println(list);
    }

    @Test
    public void getColumnMetaDataWithSql() throws Exception{
        IClient client = clientCache.getClient(DataSourceType.Spark.getPluginName());
        SqlQueryDTO sqlQueryDTO = SqlQueryDTO.builder().sql("select * from nanqi_partition").build();
        List list = client.getColumnMetaDataWithSql(source, sqlQueryDTO);
        System.out.println(list);
    }

    @Test
    public void getCreateTableSql() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.Spark.getPluginName());
        SqlQueryDTO sqlQueryDTO = SqlQueryDTO.builder().tableName("nanqi_partition").build();
        System.out.println(client.getCreateTableSql(source, sqlQueryDTO));
    }

    @Test
    public void getAllDataBases() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.Spark.getPluginName());
        SqlQueryDTO sqlQueryDTO = SqlQueryDTO.builder().build();
        System.out.println(client.getAllDatabases(source, sqlQueryDTO));
    }
}
