package com.dtstack.dtcenter.loader.client.sql;

import com.dtstack.dtcenter.loader.client.BaseTest;
import com.dtstack.dtcenter.loader.client.ClientCache;
import com.dtstack.dtcenter.loader.client.IClient;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.source.KylinSourceDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import org.apache.commons.collections.CollectionUtils;
import org.junit.Assert;
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
public class KylinTest extends BaseTest {

    // 构建client
    private static final IClient client = ClientCache.getClient(DataSourceType.Kylin.getVal());

    // 构建数据源信息
    private static final KylinSourceDTO source = KylinSourceDTO.builder()
            .url("jdbc:kylin://172.16.101.17:7070/learn_kylin")
            .username("ADMIN")
            .password("KYLIN")
            .build();

    /**
     * 数据准备
     */

    @Test
    public void getCon() throws Exception {
        Connection con1 = client.getCon(source);
        con1.close();
    }

    @Test
    public void testCon() {
        Boolean isConnected = client.testCon(source);
        if (Boolean.FALSE.equals(isConnected)) {
            throw new DtLoaderException("connection exception");
        }
    }

    @Test
    public void executeQuery() {
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("select 111").build();
        List<Map<String, Object>> mapList = client.executeQuery(source, queryDTO);
        Assert.assertTrue(CollectionUtils.isNotEmpty(mapList));
    }

    @Test
    public void executeSqlWithoutResultSet() {
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("select 111").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
    }

    @Test
    public void getTableList() {
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().build();
        List<String> tableList = client.getTableList(source, queryDTO);
        Assert.assertTrue(CollectionUtils.isNotEmpty(tableList));
    }
}
