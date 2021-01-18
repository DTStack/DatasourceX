package com.dtstack.dtcenter.loader.client.sql;

import com.dtstack.dtcenter.loader.client.ClientCache;
import com.dtstack.dtcenter.loader.client.IClient;
import com.dtstack.dtcenter.loader.client.IHbase;
import com.dtstack.dtcenter.loader.client.IKerberos;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.comparator.BinaryComparator;
import com.dtstack.dtcenter.loader.dto.filter.Filter;
import com.dtstack.dtcenter.loader.dto.filter.RowFilter;
import com.dtstack.dtcenter.loader.dto.source.HbaseSourceDTO;
import com.dtstack.dtcenter.loader.enums.CompareOp;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.dtstack.dtcenter.loader.kerberos.HadoopConfTool;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import org.apache.commons.collections.CollectionUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 16:31 2020/9/7
 * @Description：Hbase Kerberos 测试
 */
public class HbaseKerberosTest {
    // 构建client
    private static final IClient client = ClientCache.getClient(DataSourceType.HBASE.getVal());

    // 构建hbase client
    private static final IHbase HBASE_CLIENT = ClientCache.getHbase(DataSourceType.HBASE.getVal());

    // 构建数据源信息
    private static final HbaseSourceDTO source = HbaseSourceDTO.builder()
            .url("172.16.101.239:2181")
            .path("/hbase")
            .build();
    /**
     * 数据准备
     */
    @BeforeClass
    public static void setUp () {
        // 准备 Kerberos 参数
        Map<String, Object> kerberosConfig = new HashMap<>();
        kerberosConfig.put(HadoopConfTool.PRINCIPAL_FILE, "/hbase.keytab");
        kerberosConfig.put(HadoopConfTool.KEY_JAVA_SECURITY_KRB5_CONF, "/krb5.conf");
        kerberosConfig.put(HadoopConfTool.HBASE_MASTER_PRINCIPAL, "hbase/_HOST@DTSTACK.COM");
        kerberosConfig.put(HadoopConfTool.HBASE_REGION_PRINCIPAL, "hbase/_HOST@DTSTACK.COM");
        source.setKerberosConfig(kerberosConfig);
        String localKerberosPath = HbaseKerberosTest.class.getResource("/phoenix5_kerberos").getPath();
        IKerberos kerberos = ClientCache.getKerberos(DataSourceType.HBASE.getVal());
        kerberos.prepareKerberosForConnect(kerberosConfig, localKerberosPath);

        try {
            HBASE_CLIENT.createHbaseTable(source, "loader_test_1", new String[]{"info"});
        } catch (Exception e) {
            // 目前没有支持没有删除表的方法
        }
        HBASE_CLIENT.putRow(source, "loader_test_1", UUID.randomUUID().toString(), "info", "uuid", UUID.randomUUID().toString());
        HBASE_CLIENT.putRow(source, "loader_test_1", UUID.randomUUID().toString(), "info", "uuid", UUID.randomUUID().toString());
        HBASE_CLIENT.putRow(source, "loader_test_1", UUID.randomUUID().toString(), "info", "uuid", UUID.randomUUID().toString());
    }

    @Test
    public void testCon() throws Exception {
        Boolean isConnected = client.testCon(source);
        if (Boolean.FALSE.equals(isConnected)) {
            throw new DtLoaderException("连接异常");
        }
    }

    @Test
    public void getTableList() {
        List<String> tableList = client.getTableList(source, null);
        Assert.assertTrue(CollectionUtils.isNotEmpty(tableList));
    }

    @Test
    public void getColumnMetaData() {
        List metaData = client.getColumnMetaData(source, SqlQueryDTO.builder().tableName("loader_test_1").build());
        Assert.assertTrue(CollectionUtils.isNotEmpty(metaData));
    }

    @Test
    public void executorQuery() {
        ArrayList<Filter> filters = new ArrayList<>();
        RowFilter hbaseRowFilter = new RowFilter(
                CompareOp.GREATER, new BinaryComparator("0".getBytes()));
        hbaseRowFilter.setReversed(true);
        filters.add(hbaseRowFilter);
        SqlQueryDTO sqlQueryDTO = SqlQueryDTO.builder().tableName("loader_test_1").hbaseFilter(filters).build();
        List result = client.executeQuery(source, sqlQueryDTO);
        Assert.assertTrue(CollectionUtils.isNotEmpty(result));
    }

    @Test
    public void preview() {
        List<List<Object>> result = client.getPreview(source, SqlQueryDTO.builder().tableName("loader_test_1").previewNum(2).build());
        Assert.assertTrue(CollectionUtils.isNotEmpty(result));
    }
}
