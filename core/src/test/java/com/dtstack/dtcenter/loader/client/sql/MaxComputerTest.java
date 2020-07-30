package com.dtstack.dtcenter.loader.client.sql;

import com.dtstack.dtcenter.common.exception.DtCenterDefException;
import com.dtstack.dtcenter.loader.client.AbsClientCache;
import com.dtstack.dtcenter.loader.client.IClient;
import com.dtstack.dtcenter.loader.dto.ColumnMetaDTO;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.source.OdpsSourceDTO;
import com.dtstack.dtcenter.loader.enums.ClientType;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import org.junit.Test;

import java.sql.Connection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 09:52 2020/4/24
 * @Description：MaxComputer 测试
 */
public class MaxComputerTest {
    private static final AbsClientCache clientCache = ClientType.DATA_SOURCE_CLIENT.getClientCache();

    OdpsSourceDTO source = OdpsSourceDTO.builder()
            .config("{\"accessId\":\"LTAINn0gjHA3Yxy6\",\"accessKey\":\"p1xcV89FzYyCInwA6YTyYawJnTwNzh\"," +
                    "\"project\":\"dtstack_dev\",\"endPoint\":\"\"}")
            .isCache(true)
            .build();

    @Test
    public void testCon() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.MAXCOMPUTE.getPluginName());
        Boolean isConnected = client.testCon(source);
        if (Boolean.FALSE.equals(isConnected)) {
            throw new DtCenterDefException("连接异常");
        }
    }

    @Test
    public void executeQuery() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.MAXCOMPUTE.getPluginName());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("select province from wangchuan_test;").build();
        List<Map<String, Object>> mapList = client.executeQuery(source, queryDTO);
        System.out.println(mapList);
    }

    @Test
    public void executeSqlWithoutResultSet() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.MAXCOMPUTE.getPluginName());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("show tables").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
    }

    @Test
    public void getTableList() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.MAXCOMPUTE.getPluginName());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().build();
        List<String> tableList = client.getTableList(source, queryDTO);
        System.out.println(tableList.size());
    }

    @Test
    public void getColumnClassInfo() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.MAXCOMPUTE.getPluginName());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("aa_temp").build();
        List<String> columnClassInfo = client.getColumnClassInfo(source, queryDTO);
        System.out.println(columnClassInfo.size());
    }

    @Test
    public void getColumnMetaData() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.MAXCOMPUTE.getPluginName());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("wangchuan_test2").build();
        List<ColumnMetaDTO> columnMetaData = client.getColumnMetaData(source, queryDTO);
        System.out.println(columnMetaData);
    }

    @Test
    public void getTableMetaComment() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.MAXCOMPUTE.getPluginName());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("aa_temp").build();
        String metaComment = client.getTableMetaComment(source, queryDTO);
        System.out.println(metaComment);
    }

    @Test
    public void getConn() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.MAXCOMPUTE.getPluginName());
        Connection con = client.getCon(source);
        System.out.println(con);
    }

    @Test
    public void getPreview() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.MAXCOMPUTE.getPluginName());
        HashMap<String, String> map = new HashMap<>();
        map.put("province", "henan");
        map.put("province", "shandong");
        List data = client.getPreview(source, SqlQueryDTO.builder().partitionColumns(map).previewNum(1000).tableName("wangchuan_test").build());
        System.out.println(data);
    }

    @Test
    public void getColumnMetaDataWithSql() throws Exception{
        IClient client = clientCache.getClient(DataSourceType.MAXCOMPUTE.getPluginName());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("select * from wangchuan_test2 a left semi join wangchuan_test b on a.id = b.id").build();
        List data = client.getColumnMetaDataWithSql(source, queryDTO);
        System.out.println(data);
    }
}
