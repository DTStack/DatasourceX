package com.dtstack.dtcenter.loader.client.sql;

import com.dtstack.dtcenter.common.exception.DtCenterDefException;
import com.dtstack.dtcenter.loader.cache.pool.config.PoolConfig;
import com.dtstack.dtcenter.loader.client.AbsClientCache;
import com.dtstack.dtcenter.loader.client.IClient;
import com.dtstack.dtcenter.loader.dto.ColumnMetaDTO;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.source.KylinSourceDTO;
import com.dtstack.dtcenter.loader.enums.ClientType;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import org.junit.Test;

import java.sql.Connection;
import java.util.List;
import java.util.Map;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 01:25 2020/2/29
 * @Description：Kylin 测试
 */
public class KylinTest {
    private static final AbsClientCache clientCache = ClientType.DATA_SOURCE_CLIENT.getClientCache();

    KylinSourceDTO source = KylinSourceDTO.builder()
            .url("jdbc:kylin://172.16.100.105:7070/1005_9")
            .username("ADMIN")
            .password("KYLIN")
            .poolConfig(new PoolConfig())
            .build();

    @Test
    public void getCon() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.Kylin.getPluginName());
        Connection con1 = client.getCon(source);
        String con1JdbcConn = con1.toString().split("wrapping")[1];
        Connection con2 = client.getCon(source);
        Connection con3 = client.getCon(source);
        Connection con4 = client.getCon(source);
        Connection con5 = client.getCon(source);
        Connection con6 = client.getCon(source);
        Connection con7 = client.getCon(source);
        Connection con8 = client.getCon(source);
        Connection con9 = client.getCon(source);
        Connection con10 = client.getCon(source);
        con1.close();
        Connection con11 = client.getCon(source);
        String con11JdbcConn = con11.toString().split("wrapping")[1];
        assert con1JdbcConn.equals(con11JdbcConn);
        con2.close();
        con3.close();
        con4.close();
        con5.close();
        con6.close();
        con7.close();
        con8.close();
        con9.close();
        con10.close();
        con11.close();
    }

    @Test
    public void testCon() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.Kylin.getPluginName());
        Boolean isConnected = client.testCon(source);
        if (Boolean.FALSE.equals(isConnected)) {
            throw new DtCenterDefException("连接异常");
        }
    }

    @Test
    public void executeQuery() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.Kylin.getPluginName());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("select 1111").build();
        List<Map<String, Object>> mapList = client.executeQuery(source, queryDTO);
        System.out.println(mapList);
    }

    @Test
    public void executeSqlWithoutResultSet() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.Kylin.getPluginName());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("select 1111").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
    }

    @Test
    public void getTableList() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.Kylin.getPluginName());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().build();
        List<String> tableList = client.getTableList(source, queryDTO);
        System.out.println(tableList.size());
    }

    @Test
    public void getColumnClassInfo() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.Kylin.getPluginName());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("YC_VIEW_01").build();
        List<String> columnClassInfo = client.getColumnClassInfo(source, queryDTO);
        System.out.println(columnClassInfo.size());
    }

    @Test
    public void getColumnMetaData() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.Kylin.getPluginName());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("YC_VIEW_01").build();
        List<ColumnMetaDTO> columnMetaData = client.getColumnMetaData(source, queryDTO);
        System.out.println(columnMetaData.size());
    }

}
