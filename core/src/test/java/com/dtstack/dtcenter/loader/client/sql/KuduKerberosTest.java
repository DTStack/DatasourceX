package com.dtstack.dtcenter.loader.client.sql;

import com.dtstack.dtcenter.loader.client.ClientCache;
import com.dtstack.dtcenter.loader.client.IClient;
import com.dtstack.dtcenter.loader.client.IKerberos;
import com.dtstack.dtcenter.loader.dto.ColumnMetaDTO;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.source.KuduSourceDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.dtstack.dtcenter.loader.kerberos.HadoopConfTool;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import org.apache.commons.collections.CollectionUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 14:05 2020/9/10
 * @Description：Kudu Kerberos 认证
 */
public class KuduKerberosTest {
    static KuduSourceDTO source = KuduSourceDTO.builder()
            .url("172.16.100.208:7051")
            .build();

    @BeforeClass
    public static void setUp() {
        // 准备 Kerberos 参数
        Map<String, Object> kerberosConfig = new HashMap<>();
        kerberosConfig.put(HadoopConfTool.PRINCIPAL_FILE, "/kudu.keytab");
        kerberosConfig.put(HadoopConfTool.KEY_JAVA_SECURITY_KRB5_CONF, "/krb5.conf");
        source.setKerberosConfig(kerberosConfig);

        String localKerberosPath = KuduKerberosTest.class.getResource("/eng-cdh3").getPath();
        IKerberos kerberos = ClientCache.getKerberos(DataSourceType.Kudu.getVal());
        kerberos.prepareKerberosForConnect(kerberosConfig, localKerberosPath);
    }

    @Test
    public void testCon() {
        IClient client = ClientCache.getClient(DataSourceType.Kudu.getVal());
        Boolean isConnected = client.testCon(source);
        if (Boolean.FALSE.equals(isConnected)) {
            throw new DtLoaderException("connection exception");
        }
    }

    @Test
    public void getTableList() {
        IClient client = ClientCache.getClient(DataSourceType.Kudu.getVal());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().build();
        List<String> tableList = client.getTableList(source, queryDTO);
        Assert.assertTrue(CollectionUtils.isNotEmpty(tableList));
    }

    @Test
    public void getColumnMetaData() {
        IClient client = ClientCache.getClient(DataSourceType.Kudu.getVal());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("loader_test1").build();
        List<ColumnMetaDTO> columnMetaData = client.getColumnMetaData(source, queryDTO);
        Assert.assertTrue(CollectionUtils.isNotEmpty(columnMetaData));
    }
}
