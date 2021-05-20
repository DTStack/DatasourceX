package com.dtstack.dtcenter.loader.client.sql;

import com.dtstack.dtcenter.loader.client.BaseTest;
import com.dtstack.dtcenter.loader.client.ClientCache;
import com.dtstack.dtcenter.loader.client.IClient;
import com.dtstack.dtcenter.loader.client.IKerberos;
import com.dtstack.dtcenter.loader.dto.ColumnMetaDTO;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.source.Phoenix5SourceDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.dtstack.dtcenter.loader.kerberos.HadoopConfTool;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import org.apache.commons.collections.CollectionUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * phoenix5 开启 kerberos单元测试
 *
 * @author ：wangchuan
 * date：Created in 下午 05:10 2021/1/11
 * company: www.dtstack.com
 */
public class Phoenix5KerberosTest extends BaseTest {


    // 构建客户端
    private static final IClient client = ClientCache.getClient(DataSourceType.PHOENIX5.getVal());

    // 构建数据源信息
    private static final Phoenix5SourceDTO source = Phoenix5SourceDTO.builder()
            .url("jdbc:phoenix:172.16.100.208,172.16.100.217,172.16.101.169:2181:/hbase")
            .build();

    @BeforeClass
    public static void beforeClass() {
        // 准备 Kerberos 参数
        Map<String, Object> kerberosConfig = new HashMap<>();
        kerberosConfig.put(HadoopConfTool.PRINCIPAL_FILE, "/hbase.keytab");
        kerberosConfig.put(HadoopConfTool.KEY_JAVA_SECURITY_KRB5_CONF, "/krb5.conf");
        kerberosConfig.put(HadoopConfTool.HBASE_REGION_PRINCIPAL, "hbase/_HOST@DTSTACK.COM");
        kerberosConfig.put(HadoopConfTool.HBASE_MASTER_PRINCIPAL, "hbase/_HOST@DTSTACK.COM");
        source.setKerberosConfig(kerberosConfig);
        String localKerberosPath = Phoenix5KerberosTest.class.getResource("/eng-cdh3").getPath();
        IKerberos kerberos = ClientCache.getKerberos(DataSourceType.PHOENIX5.getVal());
        kerberos.prepareKerberosForConnect(kerberosConfig, localKerberosPath);

        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("drop table if exists loader_test").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("CREATE TABLE loader_test (" +
                "      state CHAR(2) NOT NULL," +
                "      city VARCHAR NOT NULL," +
                "      population BIGINT" +
                "      CONSTRAINT my_pk PRIMARY KEY (state, city))").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("UPSERT INTO loader_test (state, city, population) values ('NY','New York',8143197)").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
    }


    /**
     * 获取连接测试
     */
    @Test
    public void getCon() throws Exception{
        Connection connection = client.getCon(source);
        Assert.assertNotNull(connection);
        connection.close();
    }

    /**
     * 测试连通性测试
     */
    @Test
    public void testCon()  {
        Boolean isConnected = client.testCon(source);
        if (Boolean.FALSE.equals(isConnected)) {
            throw new DtLoaderException("connection exception");
        }
    }

    /**
     * 执行查询语句测试
     */
    @Test
    public void executeQuery()  {
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("select count(1) from loader_test").build();
        List<Map<String, Object>> result = client.executeQuery(source, queryDTO);
        Assert.assertTrue(CollectionUtils.isNotEmpty(result));
    }

    /**
     * 无结果查询测试
     */
    @Test
    public void executeSqlWithoutResultSet()  {
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("select count(1) from loader_test").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
    }

    /**
     * 获取表
     */
    @Test
    public void getTableList()  {
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().build();
        List<String> tableList = client.getTableList(source, queryDTO);
        Assert.assertTrue(CollectionUtils.isNotEmpty(tableList));
    }

    /**
     * 根据 schema获取表
     */
    @Test
    public void getTableListBySchema()  {
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().schema("default").build();
        List<String> tableList = client.getTableListBySchema(source, queryDTO);
        Assert.assertTrue(CollectionUtils.isNotEmpty(tableList));
    }

    /**
     * 获取java 标准字段属性
     */
    @Test
    public void getColumnClassInfo()  {
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("loader_test").build();
        List<String> columnClassInfo = client.getColumnClassInfo(source, queryDTO);
        Assert.assertTrue(CollectionUtils.isNotEmpty(columnClassInfo));
    }

    /**
     * 获取表字段详细信息
     */
    @Test
    public void getColumnMetaData()  {
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("loader_test").build();
        List<ColumnMetaDTO> columnMetaData = client.getColumnMetaData(source, queryDTO);
        System.out.println(columnMetaData);
    }

    /**
     * 获取表注释
     */
    @Test
    public void getTableMetaComment()  {
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("loader_test").build();
        client.getTableMetaComment(source, queryDTO);
    }

    /**
     * 数据预览测试
     */
    @Test
    public void preview() {
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("loader_test").previewNum(1).build();
        List preview = client.getPreview(source, queryDTO);
        Assert.assertTrue(CollectionUtils.isNotEmpty(preview));
    }

    /**
     * 根据sql 获取对应结果的字段信息
     */
    @Test
    public void getColumnMetaDataWithSql() {
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("select * from loader_test ").build();
        List result = client.getColumnMetaDataWithSql(source, queryDTO);
        Assert.assertTrue(CollectionUtils.isNotEmpty(result));
    }

    /**
     * 获取所有的schema
     */
    @Test
    public void getAllDatabases()  {
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().build();
        List databases = client.getAllDatabases(source, queryDTO);
        Assert.assertTrue(CollectionUtils.isEmpty(databases));
    }

    /**
     * 获取指定schema下的表
     */
    @Test
    public void searchTableAndViewBySchema ()  {
        List list = client.getTableListBySchema(source, SqlQueryDTO.builder().schema("default").build());
        Assert.assertTrue(CollectionUtils.isNotEmpty(list));
    }

}
