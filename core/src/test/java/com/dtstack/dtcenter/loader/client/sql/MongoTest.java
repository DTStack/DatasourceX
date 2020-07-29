package com.dtstack.dtcenter.loader.client.sql;

import com.dtstack.dtcenter.common.enums.DataSourceClientType;
import com.dtstack.dtcenter.common.exception.DtCenterDefException;
import com.dtstack.dtcenter.loader.client.AbsClientCache;
import com.dtstack.dtcenter.loader.client.IClient;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.source.MongoSourceDTO;
import com.dtstack.dtcenter.loader.enums.ClientType;
import org.junit.Test;

import java.util.List;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 14:07 2020/2/29
 * @Description：Mongo 测试
 */
public class MongoTest {
    private static final AbsClientCache clientCache = ClientType.DATA_SOURCE_CLIENT.getClientCache();

    MongoSourceDTO source = MongoSourceDTO.builder()
            .hostPort("172.16.8.89:27017/admin")
            //.schema("admin")
            .isCache(true)
            .username("root")
            .password("admin")
            .build();

    @Test
    public void testCon() throws Exception {
        IClient client = clientCache.getClient(DataSourceClientType.MONGODB.getPluginName());
        Boolean isConnected = client.testCon(source);
        if (Boolean.FALSE.equals(isConnected)) {
            throw new DtCenterDefException("连接异常");
        }
    }

    @Test
    public void getTableList() throws Exception {
        IClient client = clientCache.getClient(DataSourceClientType.MONGODB.getPluginName());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().build();
        List<String> tableList = client.getTableList(source, queryDTO);
        System.out.println(tableList);
    }

    @Test
    public void getDatabaseList() throws Exception {
        IClient client = clientCache.getClient(DataSourceClientType.MONGODB.getPluginName());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("system.profile").build();
        List list = client.getAllDatabases(source, queryDTO);
        System.out.println(list);
    }

    @Test
    public void getPreview() throws Exception{
        IClient client = clientCache.getClient(DataSourceClientType.MONGODB.getPluginName());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("system.users").build();
        List<List<Object>> preview = client.getPreview(source, queryDTO);
        System.out.println(preview);
    }
}
