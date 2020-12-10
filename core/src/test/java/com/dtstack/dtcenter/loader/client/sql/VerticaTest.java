package com.dtstack.dtcenter.loader.client.sql;

import com.dtstack.dtcenter.loader.client.ClientCache;
import com.dtstack.dtcenter.loader.client.IClient;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.source.VerticaSourceDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.sql.Connection;
import java.util.List;
import java.util.Map;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 14:31 2020/12/10
 * @Description：Vertica 测试
 */
@Ignore
public class VerticaTest {
    private static VerticaSourceDTO verticaSourceDTO = VerticaSourceDTO.builder()
            .url("jdbc:vertica://172.16.101.225:5433/docker")
            .username("dbadmin")
            .build();

    /**
     * 获取连接测试 - 使用连接池，默认最大开启十个连接
     * @throws Exception
     */
    @Test
    public void getCon() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.VERTICA.getVal());
        Connection con1 = client.getCon(verticaSourceDTO);
        con1.close();
    }

    @Test
    public void testCon() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.VERTICA.getVal());
        Boolean isConnected = client.testCon(verticaSourceDTO);
        Assert.assertTrue(isConnected);
    }

    @Test(expected = DtLoaderException.class)
    public void testErrorCon() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.VERTICA.getVal());
        verticaSourceDTO.setUsername("nanqi");
        client.testCon(verticaSourceDTO);
    }

    @Test
    public void executeQuery() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.VERTICA.getVal());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("select 1").build();
        List<Map<String, Object>> mapList = client.executeQuery(verticaSourceDTO, queryDTO);
        System.out.println(mapList.size());
    }

    @Test
    public void getTableList() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.VERTICA.getVal());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().build();
        List<String> tableList = client.getTableList(verticaSourceDTO, queryDTO);
        System.out.println(tableList);
    }

    @Test
    public void getTableMetaComment() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.VERTICA.getVal());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("nanqi.\"ide.nanqi\"").build();
        String metaComment = client.getTableMetaComment(verticaSourceDTO, queryDTO);
        System.out.println(metaComment);
    }

    @Test
    public void getColumnMetaData() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.VERTICA.getVal());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("nanqi.\"ide.nanqi\"").build();
        List columnMetaData = client.getColumnMetaData(verticaSourceDTO, queryDTO);
        System.out.println(columnMetaData);
    }

    /**
     * 数据预览测试
     * @throws Exception
     */
    @Test
    public void testGetPreview() throws Exception{
        IClient client = ClientCache.getClient(DataSourceType.VERTICA.getVal());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("nanqi.\"ide.nanqi\"").build();
        List preview = client.getPreview(verticaSourceDTO, queryDTO);
        System.out.println(preview);
    }
}
