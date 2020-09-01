package com.dtstack.dtcenter.loader.client.sql;

import com.dtstack.dtcenter.common.exception.DtCenterDefException;
import com.dtstack.dtcenter.loader.client.AbsClientCache;
import com.dtstack.dtcenter.loader.client.IClient;
import com.dtstack.dtcenter.loader.IDownloader;
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
 * @Description：Hive 测试
 */
public class HiveTest {
    private static final AbsClientCache clientCache = ClientType.DATA_SOURCE_CLIENT.getClientCache();

    HiveSourceDTO source = HiveSourceDTO.builder()
            .url("jdbc:hive2://172.16.100.219:10000/yeluo_test")
            .schema("yeluo_test")
            .defaultFS("hdfs://ns1")
            .config("{\n" +
                    "    \"dfs.ha.namenodes.ns1\": \"nn1,nn2\",\n" +
                    "    \"dfs.namenode.rpc-address.ns1.nn2\": \"172.16.100.219:9000\",\n" +
                    "    \"dfs.client.failover.proxy.provider.ns1\": \"org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider\",\n" +
                    "    \"dfs.namenode.rpc-address.ns1.nn1\": \"172.16.100.204:9000\",\n" +
                    "    \"dfs.nameservices\": \"ns1\"\n" +
                    "}")
            .build();

    @Test
    public void getCon() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.HIVE.getPluginName());
        Connection con = client.getCon(source);
        con.createStatement().close();
        con.close();
    }

    @Test
    public void testCon() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.HIVE.getPluginName());
        Boolean isConnected = client.testCon(source);
        if (Boolean.FALSE.equals(isConnected)) {
            throw new DtCenterDefException("连接异常");
        }
    }

    @Test
    public void executeQuery() throws Exception {
        try {
            IClient client = clientCache.getClient(DataSourceType.HIVE.getPluginName());
            SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("select * from nanqi0000000").build();
            List<Map<String, Object>> mapList = client.executeQuery(source, queryDTO);
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    @Test
    public void executeSqlWithoutResultSet() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.HIVE.getPluginName());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("show tables").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
    }

    @Test
    public void getTableList() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.HIVE.getPluginName());
        List<String> tableList = client.getTableList(source, null);
        System.out.println(tableList);
    }

    @Test
    public void getColumnClassInfo() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.HIVE.getPluginName());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("yuebai").build();
        List<String> columnClassInfo = client.getColumnClassInfo(source, queryDTO);
        System.out.println(columnClassInfo.size());
    }

    @Test
    public void getColumnMetaData() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.HIVE.getPluginName());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("yuebai").build();
        List<ColumnMetaDTO> columnMetaData = client.getColumnMetaData(source, queryDTO);
        System.out.println(columnMetaData.size());
    }

    @Test
    public void getTableMetaComment() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.HIVE.getPluginName());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("yuebai").build();
        String metaComment = client.getTableMetaComment(source, queryDTO);
        System.out.println(metaComment);
    }

    @Test
    public void getDownloader() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.HIVE.getPluginName());
        for (int i = 1; i < 8; i++) {
            System.out.println("============================wangchuan00"+i+"============================");
            SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("yeluo_test_oracle_01_24909").build();
            IDownloader downloader = client.getDownloader(source, queryDTO);
            System.out.println(downloader.getMetaInfo());
            while (!downloader.reachedEnd()){
                System.out.println(downloader.readNext());
            }
        }
    }

    @Test
    public void getPreview() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.HIVE.getPluginName());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("wangchuan002").build();
        List preview = client.getPreview(source, queryDTO);
        System.out.println(preview);
    }

    @Test
    public void getPartitionColumn() throws Exception{
        IClient client = clientCache.getClient(DataSourceType.HIVE.getPluginName());
        List<ColumnMetaDTO> data = client.getColumnMetaData(source, SqlQueryDTO.builder().tableName("wangchuan002").build());
        data.forEach(x-> System.out.println(x.getKey()+"=="+x.getPart()));
    }

    @Test
    public void getPreview2() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.HIVE.getPluginName());
        HashMap<String, String> map = new HashMap<>();
        map.put("month", "3");
        map.put("day", "5");
        List list = client.getPreview(source, SqlQueryDTO.builder().tableName("wangchuan007").partitionColumns(map).build());
        System.out.println(list);
    }

    @Test
    public void query() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.HIVE.getPluginName());
        List list = client.executeQuery(source, SqlQueryDTO.builder().sql("select month,day from wangchuan007").build());
        System.out.println(list);
    }

    @Test
    public void getColumnMetaDataWithSql() throws Exception{
        IClient client = clientCache.getClient(DataSourceType.HIVE.getPluginName());
        SqlQueryDTO sqlQueryDTO = SqlQueryDTO.builder().sql("select a.id,b.name,b.email,b.pt from userinfo_4_sanyue a left join userinfo_3 b on a.id=b.id ").build();
        List list = client.getColumnMetaDataWithSql(source, sqlQueryDTO);
        System.out.println(list);
    }

    @Test
    public void getCreateTableSql() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.HIVE.getPluginName());
        SqlQueryDTO sqlQueryDTO = SqlQueryDTO.builder().tableName("dirty_mysql_kafka11_id_1395").build();
        System.out.println(client.getCreateTableSql(source, sqlQueryDTO));
    }

    @Test
    public void getAllDataBases() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.HIVE.getPluginName());
        SqlQueryDTO sqlQueryDTO = SqlQueryDTO.builder().build();
        System.out.println(client.getAllDatabases(source, sqlQueryDTO));
    }
}
