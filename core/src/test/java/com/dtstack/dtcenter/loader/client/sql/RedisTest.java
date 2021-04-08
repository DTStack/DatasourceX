package com.dtstack.dtcenter.loader.client.sql;

import com.dtstack.dtcenter.loader.client.ClientCache;
import com.dtstack.dtcenter.loader.client.IClient;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.source.RedisSourceDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import org.junit.Test;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 14:03 2020/2/29
 * @Description：Redis 测试
 */
public class RedisTest {
    RedisSourceDTO source = RedisSourceDTO.builder()
            .hostPort("172.16.101.246:16379")
            .password("DT@Stack#123")
            .schema("1")
            .build();

    /**
     * 连通性测试
     */
    @Test
    public void testCon() {
        IClient client = ClientCache.getClient(DataSourceType.REDIS.getVal());
        Boolean isConnected = client.testCon(source);
        if (Boolean.FALSE.equals(isConnected)) {
            throw new DtLoaderException("connection exception");
        }
    }

    /**
     * 数据预览测试 - 没有插入数据的方法目前
     */
    @Test
    public void preview() {
        IClient client = ClientCache.getClient(DataSourceType.REDIS.getVal());
        client.getPreview(source, SqlQueryDTO.builder().previewNum(5).tableName("loader_test").build());
    }
}
