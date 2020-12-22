package com.dtstack.dtcenter.loader.client.sql;

import com.dtstack.dtcenter.loader.cache.pool.config.PoolConfig;
import com.dtstack.dtcenter.loader.client.ClientCache;
import com.dtstack.dtcenter.loader.client.IClient;
import com.dtstack.dtcenter.loader.dto.source.LibraSourceDTO;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import org.junit.Test;

/**
 * libra client测试
 *
 * @author ：wangchuan
 * date：Created in 10:14 上午 2020/12/7
 * company: www.dtstack.com
 */
public class LibraTest {

    private static LibraSourceDTO source = LibraSourceDTO.builder()
            .url("jdbc:postgresql://172.16.8.193:5432/database")
            .username("root")
            .password("postgresql")
            .poolConfig(PoolConfig.builder().build())
            .build();

    /**
     * 创建库测试
     */
    @Test
    public void createDb() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.LIBRA.getVal());
        assert client.createDatabase(source, "wangchuan_dev_test", "测试注释");
    }

    /**
     * 判断db是否存在
     */
    @Test
    public void isDbExists() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.LIBRA.getVal());
        assert client.isDatabaseExists(source, "wangchuan_dev_test");
    }

    /**
     * 判断db是否存在
     */
    @Test
    public void isDbNotExists() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.LIBRA.getVal());
        assert !client.isDatabaseExists(source, "wangchuan_dev_test_123");
    }


    /**
     * 判断表是否在db中
     */
    @Test
    public void isTableInDb() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.LIBRA.getVal());
        assert !client.isTableExistsInDatabase(source, "test_1", "wangchuan_dev_test");
    }
}
