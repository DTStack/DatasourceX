package com.dtstack.dtcenter.loader.client.sql;

import com.dtstack.dtcenter.common.exception.DtCenterDefException;
import com.dtstack.dtcenter.loader.IDownloader;
import com.dtstack.dtcenter.loader.client.AbsClientCache;
import com.dtstack.dtcenter.loader.client.IClient;
import com.dtstack.dtcenter.loader.dto.ColumnMetaDTO;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.source.Hive1SourceDTO;
import com.dtstack.dtcenter.loader.enums.ClientType;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.util.List;
import java.util.Map;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 00:00 2020/2/29
 * @Description：Hive 1.x 测试
 */
public class Hive1Test {
    private static final AbsClientCache clientCache = ClientType.DATA_SOURCE_CLIENT.getClientCache();

    private static Hive1SourceDTO source = Hive1SourceDTO.builder()
            .url("jdbc:hive2://172.16.8.107:10000/dev")
            .schema("dev")
            .defaultFS("hdfs://ns1")
            .config("{\n" +
                    "    \"dfs.ha.namenodes.ns1\": \"nn1,nn2\",\n" +
                    "    \"dfs.namenode.rpc-address.ns1.nn2\": \"172.16.8.107:9000\",\n" +
                    "    \"dfs.client.failover.proxy.provider.ns1\": \"org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider\",\n" +
                    "    \"dfs.namenode.rpc-address.ns1.nn1\": \"172.16.8.108:9000\",\n" +
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
        IClient client = clientCache.getClient(DataSourceType.HIVE1X.getPluginName());
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
        IClient client = clientCache.getClient(DataSourceType.HIVE1X.getPluginName());
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
        IClient client = clientCache.getClient(DataSourceType.HIVE1X.getPluginName());
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
        IClient client = clientCache.getClient(DataSourceType.HIVE1X.getPluginName());
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
        IClient client = clientCache.getClient(DataSourceType.HIVE1X.getPluginName());
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
        IClient client = clientCache.getClient(DataSourceType.HIVE1X.getPluginName());
        List<String> columns = Lists.newArrayList("name", "day", "month", "id", "year");
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
        IClient client = clientCache.getClient(DataSourceType.HIVE1X.getPluginName());
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
        IClient client = clientCache.getClient(DataSourceType.HIVE1X.getPluginName());
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
        IClient client = clientCache.getClient(DataSourceType.HIVE1X.getPluginName());
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
        IClient client = clientCache.getClient(DataSourceType.HIVE1X.getPluginName());
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
        IClient client = clientCache.getClient(DataSourceType.HIVE1X.getPluginName());
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
        IClient client = clientCache.getClient(DataSourceType.HIVE1X.getPluginName());
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
        IClient client = clientCache.getClient(DataSourceType.HIVE1X.getPluginName());
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
        IClient client = clientCache.getClient(DataSourceType.HIVE1X.getPluginName());
        Connection con = client.getCon(source);
        con.createStatement().close();
        con.close();
    }

    @Test
    public void testCon() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.HIVE1X.getPluginName());
        Boolean isConnected = client.testCon(source);
        if (Boolean.FALSE.equals(isConnected)) {
            throw new DtCenterDefException("连接异常");
        }
    }

    @Test(expected = DtLoaderException.class)
    public void testConTimeout() {
        Hive1SourceDTO source = Hive1SourceDTO.builder()
                .url("jdbc:hive2://172.16.8.107:10000/default")
                .schema("default")
                .defaultFS("hdfs://1.1.1.1")
                .username("admin")
                .build();
        IClient client = clientCache.getClient(DataSourceType.HIVE1X.getPluginName());
        Boolean isConnected = client.testCon(source);
        if (Boolean.FALSE.equals(isConnected)) {
            throw new DtCenterDefException("连接异常");
        }
    }

    @Test
    public void executeQuery() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.HIVE1X.getPluginName());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("show tables").build();
        List<Map<String, Object>> mapList = client.executeQuery(source, queryDTO);
        System.out.println(mapList.size());
    }

    @Test
    public void executeSqlWithoutResultSet() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.HIVE1X.getPluginName());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("show tables").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
    }

    @Test
    public void getTableList() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.HIVE1X.getPluginName());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().build();
        List<String> tableList = client.getTableList(source, queryDTO);
        System.out.println(tableList);
    }

    @Test
    public void getColumnClassInfo() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.HIVE1X.getPluginName());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("chener_o2").build();
        List<String> columnClassInfo = client.getColumnClassInfo(source, queryDTO);
        System.out.println(columnClassInfo.size());
    }

    @Test
    public void getColumnMetaData() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.HIVE1X.getPluginName());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("chener_o2").build();
        List<ColumnMetaDTO> columnMetaData = client.getColumnMetaData(source, queryDTO);
        System.out.println(columnMetaData.size());
    }

    @Test
    public void getTableMetaComment() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.HIVE1X.getPluginName());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("chener_o2").build();
        String metaComment = client.getTableMetaComment(source, queryDTO);
        System.out.println(metaComment);
    }

    @Test
    public void getPreview() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.HIVE1X.getPluginName());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().previewNum(2).tableName("chener").build();
        List preview = client.getPreview(source, queryDTO);
        System.out.println(preview);
    }

    @Test
    public void getCreateTableSql() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.HIVE1X.getPluginName());
        SqlQueryDTO sqlQueryDTO = SqlQueryDTO.builder().tableName("chener").build();
        System.out.println(client.getCreateTableSql(source, sqlQueryDTO));
    }

    @Test
    public void getAllDataBases() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.HIVE1X.getPluginName());
        SqlQueryDTO sqlQueryDTO = SqlQueryDTO.builder().build();
        System.out.println(client.getAllDatabases(source, sqlQueryDTO));
    }

    @Test
    public void getCurrentDatabase() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.HIVE1X.getPluginName());
        String currentDatabase = client.getCurrentDatabase(source);
        Assert.assertNotNull(currentDatabase);
    }
}
