package com.dtstack.dtcenter.loader.client.sql;

import com.dtstack.dtcenter.common.exception.DtCenterDefException;
import com.dtstack.dtcenter.common.thread.RdosThreadFactory;
import com.dtstack.dtcenter.loader.IDownloader;
import com.dtstack.dtcenter.loader.client.ClientCache;
import com.dtstack.dtcenter.loader.client.IClient;
import com.dtstack.dtcenter.loader.dto.ColumnMetaDTO;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.Table;
import com.dtstack.dtcenter.loader.dto.source.HiveSourceDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;
import org.junit.BeforeClass;
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
@Slf4j
public class HiveTest {
    private static HiveSourceDTO source = HiveSourceDTO.builder()
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

    @BeforeClass
    public static void beforeClass() throws Exception {
        System.setProperty("HADOOP_USER_NAME", "root");
        IClient client = ClientCache.getClient(DataSourceType.HIVE.getVal());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("drop table if exists nanqi").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("create table nanqi (id int, name string)").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("drop table if exists nanqi1").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("create table nanqi1 (id int, name string) COMMENT 'table comment' row format delimited fields terminated by ','").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("insert into nanqi1 values (1, 'nanqi')").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("drop table if exists wangchuan01").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("create table wangchuan01 (id int, name string) STORED AS PARQUET").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("insert into wangchuan01 values (1, 'wangchuan01'),(2,'wangchuan02')").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
    }

    @Test
    public void getCon() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.HIVE.getVal());
        Connection con = client.getCon(source);
        con.createStatement().close();
        con.close();
    }

    @Test
    public void testCon() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.HIVE.getVal());
        Boolean isConnected = client.testCon(source);
        if (Boolean.FALSE.equals(isConnected)) {
            throw new DtLoaderException("连接异常");
        }
    }

    @Test
    public void executeQuery() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.HIVE.getVal());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("show tables").build();
        List<Map<String, Object>> mapList = client.executeQuery(source, queryDTO);
        System.out.println(mapList.size());
    }

//    @Test
//    public void executeQueryForThreadTest() throws Exception {
//        try {
//            IClient client = ClientCache.getClient(DataSourceType.HIVE.getVal());
//            SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("select * from nanqi1030").build();
//            List<Map<String, Object>> mapList = client.executeQuery(source, queryDTO);
//            ExecutorService threadPool = Executors.newFixedThreadPool(6, new RdosThreadFactory("test_nanqi"));
//            for (int i = 0; i < 20000000; i++) {
//                threadPool.submit(() -> {
//                    try {
//                        client.executeQuery(source, queryDTO);
//                    } catch (Exception e) {
//
//                    }
//                });
//            }
//            Thread.sleep(1000000L);
//        } catch (Exception e) {
//            System.out.println(e.getMessage());
//        }
//    }

    @Test
    public void executeSqlWithoutResultSet() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.HIVE.getVal());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("show tables").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
    }

    @Test
    public void getTableList() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.HIVE.getVal());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().build();
        List<String> tableList = client.getTableList(source, queryDTO);
        System.out.println(tableList);
    }

    @Test
    public void getColumnClassInfo() throws Exception {
        executeSqlWithoutResultSet();
        IClient client = ClientCache.getClient(DataSourceType.HIVE.getVal());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("nanqi").build();
        List<String> columnClassInfo = client.getColumnClassInfo(source, queryDTO);
        System.out.println(columnClassInfo.size());
    }

    @Test
    public void getColumnMetaData() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.HIVE.getVal());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("nanqi").build();
        List<ColumnMetaDTO> columnMetaData = client.getColumnMetaData(source, queryDTO);
        System.out.println(columnMetaData.size());
    }

    @Test
    public void getTableMetaComment() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.HIVE.getVal());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("nanqi").build();
        String metaComment = client.getTableMetaComment(source, queryDTO);
        System.out.println(metaComment);
    }

    @Test
    public void getTableMetaComment1() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.HIVE.getVal());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("nanqi1").build();
        String metaComment = client.getTableMetaComment(source, queryDTO);
        System.out.println(metaComment);
    }

    @Test
    public void getDownloader() throws Exception {
        System.setProperty("HADOOP_USER_NAME", "root");
        IClient client = ClientCache.getClient(DataSourceType.HIVE.getVal());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("nanqi").build();
        IDownloader downloader = client.getDownloader(source, queryDTO);
        System.out.println(downloader.getMetaInfo());
        while (!downloader.reachedEnd()){
            System.out.println(downloader.readNext());
        }
    }

    @Test
    public void getDownloaderForParquet() throws Exception {
        System.setProperty("HADOOP_USER_NAME", "root");
        IClient client = ClientCache.getClient(DataSourceType.HIVE.getVal());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("wangchuan01").build();
        IDownloader downloader = client.getDownloader(source, queryDTO);
        System.out.println(downloader.getMetaInfo());
        while (!downloader.reachedEnd()){
            System.out.println(downloader.readNext());
        }
    }

    @Test
    public void getPreview() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.HIVE.getVal());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("nanqi").build();
        List preview = client.getPreview(source, queryDTO);
        System.out.println(preview);
    }

    @Test
    public void getPartitionColumn() throws Exception{
        IClient client = ClientCache.getClient(DataSourceType.HIVE.getVal());
        List<ColumnMetaDTO> data = client.getColumnMetaData(source, SqlQueryDTO.builder().tableName("nanqi").build());
        data.forEach(x-> System.out.println(x.getKey()+"=="+x.getPart()));
    }

    @Test
    public void getPreview2() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.HIVE.getVal());
        HashMap<String, String> map = new HashMap<>();
        map.put("id", "1");
        List list = client.getPreview(source, SqlQueryDTO.builder().tableName("nanqi").partitionColumns(map).build());
        System.out.println(list);
    }

    @Test
    public void query() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.HIVE.getVal());
        List list = client.executeQuery(source, SqlQueryDTO.builder().sql("desc formatted nanqi").build());
        System.out.println(list);
    }

    @Test
    public void getColumnMetaDataWithSql() throws Exception{
        IClient client = ClientCache.getClient(DataSourceType.HIVE.getVal());
        SqlQueryDTO sqlQueryDTO = SqlQueryDTO.builder().sql("select * from nanqi ").build();
        List list = client.getColumnMetaDataWithSql(source, sqlQueryDTO);
        System.out.println(list);
    }

    @Test
    public void getCreateTableSql() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.HIVE.getVal());
        SqlQueryDTO sqlQueryDTO = SqlQueryDTO.builder().tableName("nanqi").build();
        System.out.println(client.getCreateTableSql(source, sqlQueryDTO));
    }

    @Test
    public void getAllDataBases() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.HIVE.getVal());
        SqlQueryDTO sqlQueryDTO = SqlQueryDTO.builder().build();
        System.out.println(client.getAllDatabases(source, sqlQueryDTO));
    }

    @Test
    public void getTable() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.HIVE.getVal());
        Table table = client.getTable(source, SqlQueryDTO.builder().tableName("nanqi1").build());
        System.out.println(table);
    }

    @Test
    public void getCurrentDatabase() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.HIVE.getPluginName());
        String currentDatabase = client.getCurrentDatabase(source);
        Assert.assertNotNull(currentDatabase);
    }
}
