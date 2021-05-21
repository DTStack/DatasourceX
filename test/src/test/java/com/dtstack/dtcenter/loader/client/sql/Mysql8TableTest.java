package com.dtstack.dtcenter.loader.client.sql;

import com.dtstack.dtcenter.loader.cache.pool.config.PoolConfig;
import com.dtstack.dtcenter.loader.client.BaseTest;
import com.dtstack.dtcenter.loader.client.ClientCache;
import com.dtstack.dtcenter.loader.client.ITable;
import com.dtstack.dtcenter.loader.dto.source.Mysql8SourceDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class Mysql8TableTest extends BaseTest {

    /**
     * 构造mysql客户端
     */
    private static final ITable client = ClientCache.getTable(DataSourceType.MySQL.getVal());

    // 构建数据源信息
    private static final Mysql8SourceDTO source = Mysql8SourceDTO.builder()
            .url("jdbc:mysql://172.16.100.186:3306/dev")
            .username("dev")
            .password("Abc12345")
            .poolConfig(PoolConfig.builder().build())
            .build();

    @Test(expected = DtLoaderException.class)
    public void showPartitions() {
        client.showPartitions(source, "dev");
    }

    /**
     * 更改表相关参数，暂时只支持更改表注释
     */
    @Test
    public void alterTableParams() {
        Map<String,String> param = new HashMap<>();
        param.put("comment","aaa");
        client.alterTableParams(source, "LOADER_TEST", param);
    }

}
