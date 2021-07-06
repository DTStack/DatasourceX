package com.dtstack.dtcenter.loader.client.sql;

import com.dtstack.dtcenter.loader.client.BaseTest;
import com.dtstack.dtcenter.loader.client.ClientCache;
import com.dtstack.dtcenter.loader.client.IClient;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.source.OpenTSDBSourceDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import org.apache.commons.collections.CollectionUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

/**
 * OpenTSDB 测试类
 *
 * @author ：wangchuan
 * date：Created in 上午10:33 2021/6/7
 * company: www.dtstack.com
 */
public class OpenTSDBTest extends BaseTest {

    // 构建client
    private static final IClient CLIENT = ClientCache.getClient(DataSourceType.OPENTSDB.getVal());

    // 构建数据源信息
    private static final OpenTSDBSourceDTO SOURCE_DTO = OpenTSDBSourceDTO.builder()
            .url("http://172.16.23.15:4242")
            .build();

    /**
     * 连通性测试
     */
    @Test
    public void testCon() {
        Boolean isConnected = CLIENT.testCon(SOURCE_DTO);
        if (Boolean.FALSE.equals(isConnected)) {
            throw new DtLoaderException("connection exception");
        }
    }

    @Test
    public void getTableList() {
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().limit(2).build();
        List<String> tableList = CLIENT.getTableList(SOURCE_DTO, queryDTO);
        Assert.assertTrue(CollectionUtils.isNotEmpty(tableList));
    }

    @Test
    public void getVersion() {
        String version = CLIENT.getVersion(SOURCE_DTO);
        Assert.assertEquals("2.3.0", version);
    }
}
