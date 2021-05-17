package com.dtstack.dtcenter.loader.client.sql;

import com.dtstack.dtcenter.loader.IDownloader;
import com.dtstack.dtcenter.loader.client.ClientCache;
import com.dtstack.dtcenter.loader.client.IClient;
import com.dtstack.dtcenter.loader.dto.ColumnMetaDTO;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.Table;
import com.dtstack.dtcenter.loader.dto.source.InceptorSourceDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;


@Slf4j
public class InceptorTest {

    /**
     * 构造 inceptor 客户端
     */
    private static final IClient INCEPTOR_CLIENT = ClientCache.getClient(DataSourceType.INCEPTOR.getVal());

    /**
     * 构建数据源信息
     */
    private static final InceptorSourceDTO INCEPTOR_SOURCE_DTO = InceptorSourceDTO.builder()
            .url("jdbc:hive2://tdh6-node3:10000/default;principal=hive/tdh6-node3@TDH")
            .schema("default")
            .defaultFS("hdfs://nameservice1")
            .config("{\n" +
                    "    \"dfs.ha.namenodes.nameservice1\": \"nn1,nn2\",\n" +
                    "    \"dfs.namenode.rpc-address.nameservice1.nn1\": \"172.16.100.192:8020\",\n" +
                    "    \"dfs.namenode.rpc-address.nameservice1.nn2\": \"172.16.101.204:8020\",\n" +
                    "    \"dfs.client.failover.proxy.provider.nameservice1\": \"org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider\",\n" +
                    "    \"dfs.nameservices\": \"nameservice1\"\n" +
                    "}")
            .build();

    /**
     * 数据准备
     */
    @BeforeClass
    public static void beforeClass() {
        // 准备 Kerberos 参数
        Map<String, Object> kerberosConfig = new HashMap<>();
        String localKerberosPath = HiveKerberosTest.class.getResource("/tdh/inceptor").getPath();
        kerberosConfig.put("principal", "hive/tdh6-node3@TDH");
        kerberosConfig.put("principalFile", localKerberosPath + "/inceptor.keytab");
        kerberosConfig.put("java.security.krb5.conf", localKerberosPath + "/krb5.conf");
        INCEPTOR_SOURCE_DTO.setKerberosConfig(kerberosConfig);

        // inceptor 不支持单条插入语法 - 只有当底层存储是星环hbase、es、事务orc表才支持。
        // 此处先将文件上传到hdfs上再使用 load data 语法进行倒入数据 - 导入数据失败。。。手动去机器上传的
        // 无法多次执行，上传测试数据比较麻烦，此处 loader_test_text 默认数据已经上传完成
        //SqlQueryDTO textQueryDTO = SqlQueryDTO.builder().sql("drop table if exists loader_test_text").build();
        //INCEPTOR_CLIENT.executeSqlWithoutResultSet(INCEPTOR_SOURCE_DTO, textQueryDTO);
        //textQueryDTO = SqlQueryDTO.builder().sql("create table loader_test_text (id int comment 'id comment', name string) COMMENT 'table comment' row format delimited fields terminated by ',' stored as TEXTFILE").build();
        //INCEPTOR_CLIENT.executeSqlWithoutResultSet(INCEPTOR_SOURCE_DTO, textQueryDTO);

        /*------------orc表--------------*/
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("drop table if exists loader_test_orc").build();
        INCEPTOR_CLIENT.executeSqlWithoutResultSet(INCEPTOR_SOURCE_DTO, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("create table loader_test_orc stored as ORC as select id,name from loader_test_text").build();
        INCEPTOR_CLIENT.executeSqlWithoutResultSet(INCEPTOR_SOURCE_DTO, queryDTO);
        /*------------csv表--------------*/
        queryDTO = SqlQueryDTO.builder().sql("drop table if exists loader_test_csv").build();
        INCEPTOR_CLIENT.executeSqlWithoutResultSet(INCEPTOR_SOURCE_DTO, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("create table loader_test_csv stored as CSVFILE as select id,name from loader_test_text").build();
        INCEPTOR_CLIENT.executeSqlWithoutResultSet(INCEPTOR_SOURCE_DTO, queryDTO);
        /*------------暂时不考虑es、hbase、orc事务表--------------*/
    }

    /**
     * 获取连接测试
     */
    @Test
    public void getCon() throws Exception {
        Connection con = INCEPTOR_CLIENT.getCon(INCEPTOR_SOURCE_DTO);
        Assert.assertNotNull(con);
        con.close();
    }

    @Test
    public void executeQuery() {
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("show tables").build();
        List<Map<String, Object>> mapList = INCEPTOR_CLIENT.executeQuery(INCEPTOR_SOURCE_DTO, queryDTO);
        Assert.assertTrue(CollectionUtils.isNotEmpty(mapList));
    }

    @Test
    public void executeSqlWithoutResultSet() {
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("show tables").build();
        INCEPTOR_CLIENT.executeSqlWithoutResultSet(INCEPTOR_SOURCE_DTO, queryDTO);
    }

    @Test
    public void getPreview() {
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("loader_test_text").build();
        List preview = INCEPTOR_CLIENT.getPreview(INCEPTOR_SOURCE_DTO, queryDTO);
        Assert.assertTrue(CollectionUtils.isNotEmpty(preview));
    }

    @Test
    public void getDownloader() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.INCEPTOR.getVal());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("select * from loader_test_text").build();
        IDownloader downloader = client.getDownloader(INCEPTOR_SOURCE_DTO, queryDTO);
        Assert.assertTrue(CollectionUtils.isNotEmpty(downloader.getMetaInfo()));
        while (!downloader.reachedEnd()) {
            Assert.assertNotNull(downloader.readNext());
        }
    }

    @Test
    public void testCon() {
        Boolean isConnected = INCEPTOR_CLIENT.testCon(INCEPTOR_SOURCE_DTO);
        if (Boolean.FALSE.equals(isConnected)) {
            throw new DtLoaderException("connection exception");
        }
    }

    @Test
    public void getTableList() {
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().build();
        List<String> tableList = INCEPTOR_CLIENT.getTableList(INCEPTOR_SOURCE_DTO, queryDTO);
        Assert.assertTrue(CollectionUtils.isNotEmpty(tableList));
    }

    @Test
    public void getTableListBySchema() {
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().schema("loader_test").build();
        List<String> tableList = INCEPTOR_CLIENT.getTableListBySchema(INCEPTOR_SOURCE_DTO, queryDTO);
        Assert.assertTrue(CollectionUtils.isEmpty(tableList));
    }

    @Test
    public void getColumnClassInfo() {
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("loader_test_text").build();
        List<String> columnClassInfo = INCEPTOR_CLIENT.getColumnClassInfo(INCEPTOR_SOURCE_DTO, queryDTO);
        Assert.assertTrue(CollectionUtils.isNotEmpty(columnClassInfo));
    }

    @Test
    public void getColumnMetaData() {
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("loader_test_text").build();
        List<ColumnMetaDTO> columnMetaData = INCEPTOR_CLIENT.getColumnMetaData(INCEPTOR_SOURCE_DTO, queryDTO);
        Assert.assertEquals("int", columnMetaData.get(0).getType());
        Assert.assertEquals("string", columnMetaData.get(1).getType());
        Assert.assertTrue(CollectionUtils.isNotEmpty(columnMetaData));
    }

    @Test
    public void getTableMetaComment() {
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("loader_test_text").build();
        String comment = INCEPTOR_CLIENT.getTableMetaComment(INCEPTOR_SOURCE_DTO, queryDTO);
        Assert.assertEquals("table comment", comment);
    }

    @Test
    public void getDownloaderForCsv() throws Exception {
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("select * from loader_test_csv").build();
        IDownloader downloader = INCEPTOR_CLIENT.getDownloader(INCEPTOR_SOURCE_DTO, queryDTO);
        Assert.assertTrue(CollectionUtils.isNotEmpty(downloader.getMetaInfo()));
        while (!downloader.reachedEnd()) {
            Assert.assertNotNull(downloader.readNext());
        }
    }

    @Test
    public void getPartitionColumn() {
        List<ColumnMetaDTO> data = INCEPTOR_CLIENT.getColumnMetaData(INCEPTOR_SOURCE_DTO, SqlQueryDTO.builder().tableName("loader_test_text").build());
        data.forEach(x -> System.out.println(x.getKey() + "==" + x.getPart()));
    }

    @Test
    public void getPreview2() {
        HashMap<String, String> map = new HashMap<>();
        map.put("id", "1");
        List list = INCEPTOR_CLIENT.getPreview(INCEPTOR_SOURCE_DTO, SqlQueryDTO.builder().tableName("loader_test_text").partitionColumns(map).build());
        Assert.assertTrue(CollectionUtils.isNotEmpty(list));
    }

    @Test
    public void getColumnMetaDataWithSql() {
        SqlQueryDTO sqlQueryDTO = SqlQueryDTO.builder().sql("select * from loader_test_text limit 1").build();
        List list = INCEPTOR_CLIENT.getColumnMetaDataWithSql(INCEPTOR_SOURCE_DTO, sqlQueryDTO);
        Assert.assertTrue(CollectionUtils.isNotEmpty(list));
    }

    @Test
    public void getCreateTableSql() {
        SqlQueryDTO sqlQueryDTO = SqlQueryDTO.builder().tableName("loader_test_text").build();
        String createTableSql = INCEPTOR_CLIENT.getCreateTableSql(INCEPTOR_SOURCE_DTO, sqlQueryDTO);
        Assert.assertTrue(StringUtils.isNotBlank(createTableSql));
    }

    @Test
    public void getAllDataBases() {
        SqlQueryDTO sqlQueryDTO = SqlQueryDTO.builder().build();
        List databases = INCEPTOR_CLIENT.getAllDatabases(INCEPTOR_SOURCE_DTO, sqlQueryDTO);
        Assert.assertTrue(CollectionUtils.isNotEmpty(databases));
    }

    @Test
    public void getTableText() {
        Table table = INCEPTOR_CLIENT.getTable(INCEPTOR_SOURCE_DTO, SqlQueryDTO.builder().tableName("loader_test_text").build());
        Assert.assertEquals(table.getStoreType(), "text");
    }

    @Test
    public void getTableCsv() {
        Table table = INCEPTOR_CLIENT.getTable(INCEPTOR_SOURCE_DTO, SqlQueryDTO.builder().tableName("loader_test_csv").build());
        Assert.assertEquals(table.getStoreType(), "csv");
    }

    @Test
    public void getTableOrc() {
        Table table = INCEPTOR_CLIENT.getTable(INCEPTOR_SOURCE_DTO, SqlQueryDTO.builder().tableName("loader_test_orc").build());
        Assert.assertEquals(table.getStoreType(), "orc");
    }

    @Test
    public void getTableLocation() {
        Table table = INCEPTOR_CLIENT.getTable(INCEPTOR_SOURCE_DTO, SqlQueryDTO.builder().tableName("loader_test_text").build());
        Assert.assertNotNull(table.getPath());
    }

    @Test
    public void createDb() {
        try {
            INCEPTOR_CLIENT.executeSqlWithoutResultSet(INCEPTOR_SOURCE_DTO, SqlQueryDTO.builder().sql("drop database if exists loader_test").build());
            assert INCEPTOR_CLIENT.createDatabase(INCEPTOR_SOURCE_DTO, "loader_test", "测试注释");
        } catch (Exception e) {
            // 可能失败
            System.out.println(e.getMessage());
        }
    }

    @Test
    public void isDbExists() {
        assert INCEPTOR_CLIENT.isDatabaseExists(INCEPTOR_SOURCE_DTO, "default");
    }

    @Test
    public void tableInDb() {
        assert INCEPTOR_CLIENT.isTableExistsInDatabase(INCEPTOR_SOURCE_DTO, "loader_test_text", "default");
    }

    @Test
    public void tableNotInDb() {
        assert !INCEPTOR_CLIENT.isTableExistsInDatabase(INCEPTOR_SOURCE_DTO, UUID.randomUUID().toString(), "default");
    }
}
