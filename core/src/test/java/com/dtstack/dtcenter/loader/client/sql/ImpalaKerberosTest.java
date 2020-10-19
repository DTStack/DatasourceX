package com.dtstack.dtcenter.loader.client.sql;

import com.dtstack.dtcenter.loader.client.ClientCache;
import com.dtstack.dtcenter.loader.client.IClient;
import com.dtstack.dtcenter.loader.client.IKerberos;
import com.dtstack.dtcenter.loader.dto.ColumnMetaDTO;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.source.ImpalaSourceDTO;
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
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 10:33 2020/9/10
 * @Description：Impala Kerberos 测试
 */
public class ImpalaKerberosTest {
    private static ImpalaSourceDTO source = ImpalaSourceDTO.builder()
            .url("jdbc:impala://eng-cdh3:21050;AuthMech=1;KrbServiceName=impala;KrbHostFQDN=eng-cdh3")
            .schema("dev")
            .build();

    @BeforeClass
    public static void beforeClass() throws Exception {
        // 准备 Kerberos 参数
        Map<String, Object> kerberosConfig = new HashMap<>();
        kerberosConfig.put(HadoopConfTool.PRINCIPAL, "impala/eng-cdh3@DTSTACK.COM");
        kerberosConfig.put(HadoopConfTool.PRINCIPAL_FILE, "/impalad-cdh3.keytab");
        kerberosConfig.put(HadoopConfTool.KEY_JAVA_SECURITY_KRB5_CONF, "/krb5.conf");
        source.setKerberosConfig(kerberosConfig);

        String localKerberosPath = ImpalaKerberosTest.class.getResource("/eng-cdh").getPath();
        IKerberos kerberos = ClientCache.getKerberos(DataSourceType.IMPALA.getVal());
        kerberos.prepareKerberosForConnect(kerberosConfig, localKerberosPath);

        IClient client = ClientCache.getClient(DataSourceType.IMPALA.getVal());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("drop table if exists nanqi").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("create table nanqi (id int, name string) comment 'table comment'").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("insert into nanqi values (1, 'nanqi')").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
    }

    @Test
    public void getCon() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.IMPALA.getVal());
        Connection con1 = client.getCon(source);
        con1.close();
    }

    @Test
    public void testCon() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.IMPALA.getVal());
        Boolean isConnected = client.testCon(source);
        if (Boolean.FALSE.equals(isConnected)) {
            throw new DtLoaderException("连接异常");
        }
    }

    @Test
    public void executeQuery() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.IMPALA.getVal());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("show tables").build();
        List<Map<String, Object>> mapList = client.executeQuery(source, queryDTO);
        System.out.println(mapList.size());
    }

    @Test
    public void executeSqlWithoutResultSet() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.IMPALA.getVal());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("show tables").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
    }

    @Test
    public void getTableList() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.IMPALA.getVal());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().build();
        List<String> tableList = client.getTableList(source, queryDTO);
        System.out.println(tableList);
    }

    @Test
    public void getColumnClassInfo() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.IMPALA.getVal());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("nanqi").build();
        List<String> columnClassInfo = client.getColumnClassInfo(source, queryDTO);
        System.out.println(columnClassInfo.size());
    }

    @Test
    public void getColumnMetaData() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.IMPALA.getVal());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("nanqi").build();
        List<ColumnMetaDTO> columnMetaData = client.getColumnMetaData(source, queryDTO);
        System.out.println(columnMetaData.size());
    }

    @Test
    public void getTableMetaComment() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.IMPALA.getVal());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("nanqi").build();
        String tableMetaComment = client.getTableMetaComment(source, queryDTO);
        System.out.println(tableMetaComment);
    }

    @Test
    public void getPreview() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.IMPALA.getVal());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().previewNum(2).tableName("nanqi").build();
        List preview = client.getPreview(source, queryDTO);
        System.out.println(preview);
    }

    @Test
    public void getAllDatabases() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.IMPALA.getVal());
        System.out.println(client.getAllDatabases(source, SqlQueryDTO.builder().build()));
    }

    @Test
    public void getPartitionColumn() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.IMPALA.getVal());
        System.out.println(client.getPartitionColumn(source, SqlQueryDTO.builder().tableName("nanqi").build()));
    }

    @Test
    public void getCreateSql() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.IMPALA.getVal());
        System.out.println(client.getCreateTableSql(source, SqlQueryDTO.builder().tableName("nanqi").build()));
    }
}
