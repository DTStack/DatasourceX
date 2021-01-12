package com.dtstack.dtcenter.loader.client.sql;

import com.dtstack.dtcenter.loader.client.ClientCache;
import com.dtstack.dtcenter.loader.client.IClient;
import com.dtstack.dtcenter.loader.client.IKerberos;
import com.dtstack.dtcenter.loader.dto.ColumnMetaDTO;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.source.Phoenix5SourceDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.dtstack.dtcenter.loader.kerberos.HadoopConfTool;
import com.dtstack.dtcenter.loader.source.DataSourceType;
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
public class Phoenix5KerberosTest {
    private static Phoenix5SourceDTO source = Phoenix5SourceDTO.builder()
            .url("jdbc:phoenix:172.16.101.239:2181:/hbase")
            .build();

    @BeforeClass
    public static void beforeClass() {
        // 准备 Kerberos 参数
        Map<String, Object> kerberosConfig = new HashMap<>();
        //kerberosConfig.put(HadoopConfTool.PRINCIPAL, "hbase/master@DTSTACK.COM");
        kerberosConfig.put(HadoopConfTool.PRINCIPAL_FILE, "/hbase.keytab");
        kerberosConfig.put(HadoopConfTool.KEY_JAVA_SECURITY_KRB5_CONF, "/krb5.conf");
        kerberosConfig.put(HadoopConfTool.HBASE_MASTER_PRINCIPAL, "hbase/_HOST@DTSTACK.COM");
        kerberosConfig.put(HadoopConfTool.HBASE_REGION_PRINCIPAL, "hbase/_HOST@DTSTACK.COM");
        source.setKerberosConfig(kerberosConfig);
        String localKerberosPath = Phoenix5KerberosTest.class.getResource("/phoenix5_kerberos").getPath();
        IKerberos kerberos = ClientCache.getKerberos(DataSourceType.PHOENIX5.getVal());
        kerberos.prepareKerberosForConnect(kerberosConfig, localKerberosPath);

        IClient client = ClientCache.getClient(DataSourceType.PHOENIX5.getVal());

        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("drop table if exists px_test1").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("CREATE TABLE px_test1 (\n" +
                "      state CHAR(2) NOT NULL,\n" +
                "      city VARCHAR NOT NULL,\n" +
                "      population BIGINT\n" +
                "      CONSTRAINT my_pk PRIMARY KEY (state, city))").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("UPSERT INTO px_test1 (state, city, population) values ('NY','New York',8143197)").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
    }


    @Test
    public void getCon() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.PHOENIX5.getVal());
        Connection con = client.getCon(source);
        con.createStatement().close();
        con.close();
    }

    @Test
    public void testCon() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.PHOENIX5.getVal());
        Boolean isConnected = client.testCon(source);
        if (Boolean.FALSE.equals(isConnected)) {
            throw new DtLoaderException("连接异常");
        }
    }

    @Test
    public void executeQuery() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.PHOENIX5.getVal());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("SELECT * FROM PX_TEST1").build();
        List<Map<String, Object>> mapList = client.executeQuery(source, queryDTO);
        System.out.println(mapList);
    }

    @Test
    public void executeSqlWithoutResultSet() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.PHOENIX5.getVal());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("select * from PX_TEST1 limit 2000").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
    }

    @Test
    public void getTableList() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.PHOENIX5.getVal());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().view(true).build();
        List<String> tableList = client.getTableList(source, queryDTO);
        System.out.println(tableList);
    }

    @Test
    public void getColumnClassInfo() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.PHOENIX5.getVal());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("PX_TEST1").build();
        List<String> columnClassInfo = client.getColumnClassInfo(source, queryDTO);
        System.out.println(columnClassInfo.size());
    }

    @Test
    public void getColumnMetaData() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.PHOENIX5.getVal());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("PX_TEST1").build();
        List<ColumnMetaDTO> columnMetaData = client.getColumnMetaData(source, queryDTO);
        System.out.println(columnMetaData.size());
    }

    @Test
    public void getTableMetaComment() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.PHOENIX5.getVal());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("PX_TEST1").build();
        String metaComment = client.getTableMetaComment(source, queryDTO);
        System.out.println(metaComment);
    }

    @Test
    public void getAllSchema() {
        IClient client = ClientCache.getClient(DataSourceType.PHOENIX5.getVal());
        List allSchema = client.getAllDatabases(source, SqlQueryDTO.builder().build());
        System.out.println(allSchema);
    }
}
