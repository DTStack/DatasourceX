package com.dtstack.dtcenter.loader.client.sql;

import com.dtstack.dtcenter.common.exception.DtCenterDefException;
import com.dtstack.dtcenter.common.thread.RdosThreadFactory;
import com.dtstack.dtcenter.loader.IDownloader;
import com.dtstack.dtcenter.loader.client.AbsClientCache;
import com.dtstack.dtcenter.loader.client.IClient;
import com.dtstack.dtcenter.loader.dto.ColumnMetaDTO;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.source.HiveSourceDTO;
import com.dtstack.dtcenter.loader.enums.ClientType;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 00:13 2020/2/29
 * @Description：Hive 测试
 */
public class HiveTest {
    private static final AbsClientCache clientCache = ClientType.DATA_SOURCE_CLIENT.getClientCache();

    private static HiveSourceDTO source = HiveSourceDTO.builder()
            .url("jdbc:hive2://kudu1:10000/dev")
            .schema("dev")
            .defaultFS("hdfs://ns1")
            .config("{\n" +
                    "    \"dfs.ha.namenodes.ns1\": \"nn1,nn2\",\n" +
                    "    \"dfs.namenode.rpc-address.ns1.nn2\": \"kudu1:9000\",\n" +
                    "    \"dfs.client.failover.proxy.provider.ns1\": \"org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider\",\n" +
                    "    \"dfs.namenode.rpc-address.ns1.nn1\": \"kudu2:9000\",\n" +
                    "    \"dfs.nameservices\": \"ns1\"\n" +
                    "}")
            .username("admin")
            .build();

    /**
     * hive数据下载准备
     * @throws Exception
     */
    @BeforeClass
    public static void prepareForDownloader() throws Exception {
        System.setProperty("HADOOP_USER_NAME", "admin");
        IClient client = clientCache.getClient(DataSourceType.HIVE.getPluginName());
        String deleteSqlText = "drop table if exists loader_test_text";
        String createSqlText = "create table if not exists loader_test_text (id int, name string) partitioned by (year string,month string,day string) stored as textfile";
        String insertSql1Text = "insert into loader_test_text partition (year = '2020', month = '11', day = '24') values (1, 'wangchuan'),(2, 'wangbin'),(3, 'poxiao')";
        String insertSql2Text = "insert into loader_test_text partition (year = '2021', month = '12', day = '25') values (4, 'wangchuan'),(5, 'wangbin'),(6, 'poxiao')";
        client.executeSqlWithoutResultSet(source, SqlQueryDTO.builder().sql(deleteSqlText).build());
        client.executeSqlWithoutResultSet(source, SqlQueryDTO.builder().sql(createSqlText).build());
        client.executeSqlWithoutResultSet(source, SqlQueryDTO.builder().sql(insertSql1Text).build());
        client.executeSqlWithoutResultSet(source, SqlQueryDTO.builder().sql(insertSql2Text).build());

        String deleteSqlParquet = "drop table if exists loader_test_parquet";
        String createSqlParquet = "create table if not exists loader_test_parquet (id int, name string) partitioned by (year string,month string,day string) stored as parquet";
        String insertSql1Parquet = "insert into loader_test_parquet partition (year = '2020', month = '11', day = '24') values (1, 'wangchuan'),(2, 'wangbin'),(3, 'poxiao')";
        String insertSql2Parquet = "insert into loader_test_parquet partition (year = '2021', month = '12', day = '25') values (4, 'wangchuan'),(5, 'wangbin'),(6, 'poxiao')";
        client.executeSqlWithoutResultSet(source, SqlQueryDTO.builder().sql(deleteSqlParquet).build());
        client.executeSqlWithoutResultSet(source, SqlQueryDTO.builder().sql(createSqlParquet).build());
        client.executeSqlWithoutResultSet(source, SqlQueryDTO.builder().sql(insertSql1Parquet).build());
        client.executeSqlWithoutResultSet(source, SqlQueryDTO.builder().sql(insertSql2Parquet).build());

        String deleteSqlOrc = "drop table if exists loader_test_orc";
        String createSqlOrc = "create table if not exists loader_test_orc (id int, name string) partitioned by (year string,month string,day string) stored as orc";
        String insertSql1Orc = "insert into loader_test_orc partition (year = '2020', month = '11', day = '24') values (1, 'wangchuan'),(2, 'wangbin'),(3, 'poxiao')";
        String insertSql2Orc = "insert into loader_test_orc partition (year = '2021', month = '12', day = '25') values (4, 'wangchuan'),(5, 'wangbin'),(6, 'poxiao')";
        client.executeSqlWithoutResultSet(source, SqlQueryDTO.builder().sql(deleteSqlOrc).build());
        client.executeSqlWithoutResultSet(source, SqlQueryDTO.builder().sql(createSqlOrc).build());
        client.executeSqlWithoutResultSet(source, SqlQueryDTO.builder().sql(insertSql1Orc).build());
        client.executeSqlWithoutResultSet(source, SqlQueryDTO.builder().sql(insertSql2Orc).build());
    }

    /**
     * textFile格式的hive表数据指定列下载
     * @throws Exception
     */
    @Test
    public void textDownloadWithColumn () throws Exception {
        IClient client = clientCache.getClient(DataSourceType.HIVE.getPluginName());
        List<String> columns = Lists.newArrayList("name", "day", "month", "id", "year");
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("loader_test_text").columns(columns).build();
        IDownloader downloader = client.getDownloader(source, queryDTO);
        System.out.println(columns);
        System.out.println("------------------------------------");
        while (!downloader.reachedEnd()) {
            System.out.println(downloader.readNext());
            System.out.println("------------------------------------");
        }
    }

    /**
     * textFile格式的hive表数据不指定列下载，column中传*，下载所有列数据
     * @throws Exception
     */
    @Test
    public void textDownloadWithAll () throws Exception {
        IClient client = clientCache.getClient(DataSourceType.HIVE.getPluginName());
        List<String> columns = Lists.newArrayList("*");
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("loader_test_text").columns(columns).build();
        IDownloader downloader = client.getDownloader(source, queryDTO);
        System.out.println(downloader.getMetaInfo());
        System.out.println("------------------------------------");
        while (!downloader.reachedEnd()) {
            System.out.println(downloader.readNext());
            System.out.println("------------------------------------");
        }
    }

    /**
     * textFile格式的hive表数据不指定列下载，column传null，下载所有列数据
     * @throws Exception
     */
    @Test
    public void textDownloadWithNull () throws Exception {
        IClient client = clientCache.getClient(DataSourceType.HIVE.getPluginName());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("loader_test_text").columns(null).build();
        IDownloader downloader = client.getDownloader(source, queryDTO);
        System.out.println(downloader.getMetaInfo());
        System.out.println("------------------------------------");
        while (!downloader.reachedEnd()) {
            System.out.println(downloader.readNext());
            System.out.println("------------------------------------");
        }
    }

    /**
     * textFile格式的hive表数据指定不存在的列下载 - 异常测试
     * @throws Exception
     */
    @Test(expected = Exception.class)
    public void textDownloadWithColumnNotExists () throws Exception {
        IClient client = clientCache.getClient(DataSourceType.HIVE.getPluginName());
        List<String> columns = Lists.newArrayList("not_exists_column");
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("loader_test_text").columns(columns).build();
        IDownloader downloader = client.getDownloader(source, queryDTO);
        System.out.println(downloader.getMetaInfo());
        System.out.println("------------------------------------");
        while (!downloader.reachedEnd()) {
            System.out.println(downloader.readNext());
            System.out.println("------------------------------------");
        }
    }

    /**
     * parquet格式的hive表数据指定列下载
     * @throws Exception
     */
    @Test
    public void parquetDownloadWithColumn () throws Exception {
        IClient client = clientCache.getClient(DataSourceType.HIVE.getPluginName());
        List<String> columns = Lists.newArrayList("day", "month", "id", "year");
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("loader_test_parquet").columns(columns).build();
        IDownloader downloader = client.getDownloader(source, queryDTO);
        System.out.println(columns);
        System.out.println("------------------------------------");
        while (!downloader.reachedEnd()) {
            System.out.println(downloader.readNext());
            System.out.println("------------------------------------");
        }
    }

    /**
     * parquet格式的hive表数据不指定列下载，column中传*，下载所有列数据
     * @throws Exception
     */
    @Test
    public void parquetDownloadWithAll () throws Exception {
        IClient client = clientCache.getClient(DataSourceType.HIVE.getPluginName());
        List<String> columns = Lists.newArrayList("*");
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("loader_test_parquet").columns(columns).build();
        IDownloader downloader = client.getDownloader(source, queryDTO);
        System.out.println(downloader.getMetaInfo());
        System.out.println("------------------------------------");
        while (!downloader.reachedEnd()) {
            System.out.println(downloader.readNext());
            System.out.println("------------------------------------");
        }
    }

    /**
     * parquet格式的hive表数据不指定列下载，column传null，下载所有列数据
     * @throws Exception
     */
    @Test
    public void parquetDownloadWithNull () throws Exception {
        IClient client = clientCache.getClient(DataSourceType.HIVE.getPluginName());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("loader_test_parquet").columns(null).build();
        IDownloader downloader = client.getDownloader(source, queryDTO);
        System.out.println(downloader.getMetaInfo());
        System.out.println("------------------------------------");
        while (!downloader.reachedEnd()) {
            System.out.println(downloader.readNext());
            System.out.println("------------------------------------");
        }
    }

    /**
     * parquet格式的hive表数据指定不存在的列下载 - 异常测试
     * @throws Exception
     */
    @Test(expected = Exception.class)
    public void parquetDownloadWithColumnNotExists () throws Exception {
        IClient client = clientCache.getClient(DataSourceType.HIVE.getPluginName());
        List<String> columns = Lists.newArrayList("not_exists_column");
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("loader_test_parquet").columns(columns).build();
        IDownloader downloader = client.getDownloader(source, queryDTO);
        System.out.println(downloader.getMetaInfo());
        System.out.println("------------------------------------");
        while (!downloader.reachedEnd()) {
            System.out.println(downloader.readNext());
            System.out.println("------------------------------------");
        }
    }

    /**
     * orc格式的hive表数据指定列下载
     * @throws Exception
     */
    @Test
    public void orcDownloadWithColumn () throws Exception {
        IClient client = clientCache.getClient(DataSourceType.HIVE.getPluginName());
        List<String> columns = Lists.newArrayList("name", "day", "month", "id", "year");
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("loader_test_orc").columns(columns).build();
        IDownloader downloader = client.getDownloader(source, queryDTO);
        System.out.println(columns);
        System.out.println("------------------------------------");
        while (!downloader.reachedEnd()) {
            System.out.println(downloader.readNext());
            System.out.println("------------------------------------");
        }
    }

    /**
     * orc格式的hive表数据不指定列下载，column中传*，下载所有列数据
     * @throws Exception
     */
    @Test
    public void orcDownloadWithAll () throws Exception {
        IClient client = clientCache.getClient(DataSourceType.HIVE.getPluginName());
        List<String> columns = Lists.newArrayList("*");
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("loader_test_orc").columns(columns).build();
        IDownloader downloader = client.getDownloader(source, queryDTO);
        System.out.println(downloader.getMetaInfo());
        System.out.println("------------------------------------");
        while (!downloader.reachedEnd()) {
            System.out.println(downloader.readNext());
            System.out.println("------------------------------------");
        }
    }

    /**
     * orc格式的hive表数据不指定列下载，column传null，下载所有列数据
     * @throws Exception
     */
    @Test
    public void orcDownloadWithNull () throws Exception {
        IClient client = clientCache.getClient(DataSourceType.HIVE.getPluginName());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("loader_test_orc").columns(null).build();
        IDownloader downloader = client.getDownloader(source, queryDTO);
        System.out.println(downloader.getMetaInfo());
        System.out.println("------------------------------------");
        while (!downloader.reachedEnd()) {
            System.out.println(downloader.readNext());
            System.out.println("------------------------------------");
        }
    }

    /**
     * orc格式的hive表数据指定不存在的列下载 - 异常测试
     * @throws Exception
     */
    @Test(expected = Exception.class)
    public void orcDownloadWithColumnNotExists () throws Exception {
        IClient client = clientCache.getClient(DataSourceType.HIVE.getPluginName());
        List<String> columns = Lists.newArrayList("not_exists_column");
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("loader_test_orc").columns(columns).build();
        IDownloader downloader = client.getDownloader(source, queryDTO);
        System.out.println(downloader.getMetaInfo());
        System.out.println("------------------------------------");
        while (!downloader.reachedEnd()) {
            System.out.println(downloader.readNext());
            System.out.println("------------------------------------");
        }
    }

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

    @Test(expected = DtLoaderException.class)
    public void testConTimeout() throws Exception {
        HiveSourceDTO source = HiveSourceDTO.builder()
                .url("jdbc:hive2://172.16.100.219:10000/yeluo_test")
                .schema("yeluo_test")
                .defaultFS("hdfs://1.1.1.1")
                .build();
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
            SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("select * from test_wangccc").build();
            List<Map<String, Object>> mapList = client.executeQuery(source, queryDTO);
            System.out.println(mapList);
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    @Test
    public void executeQueryForThreadTest() throws Exception {
        try {
            IClient client = clientCache.getClient(DataSourceType.HIVE.getPluginName());
            SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("select * from nanqi1030").build();
            List<Map<String, Object>> mapList = client.executeQuery(source, queryDTO);
            ExecutorService threadPool = Executors.newFixedThreadPool(6, new RdosThreadFactory("test_nanqi"));
            for (int i = 0; i < 20000000; i++) {
                threadPool.submit(() -> {
                    try {
                        client.executeQuery(source, queryDTO);
                    } catch (Exception e) {

                    }
                });
            }
            Thread.sleep(1000000L);
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

    @Test
    public void getCurrentDatabase() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.HIVE.getPluginName());
        String currentDatabase = client.getCurrentDatabase(source);
        Assert.assertNotNull(currentDatabase);
    }
}
