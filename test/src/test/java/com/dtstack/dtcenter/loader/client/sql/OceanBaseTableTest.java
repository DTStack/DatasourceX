package com.dtstack.dtcenter.loader.client.sql;

import com.dtstack.dtcenter.loader.cache.pool.config.PoolConfig;
import com.dtstack.dtcenter.loader.client.BaseTest;
import com.dtstack.dtcenter.loader.client.ClientCache;
import com.dtstack.dtcenter.loader.client.ITable;
import com.dtstack.dtcenter.loader.dto.UpsertColumnMetaDTO;
import com.dtstack.dtcenter.loader.dto.source.OceanBaseSourceDTO;
import com.dtstack.dtcenter.loader.enums.CommandType;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import org.apache.commons.collections.CollectionUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class OceanBaseTableTest extends BaseTest {

    /**
     * 构造mysql客户端
     */
    private static final ITable client = ClientCache.getTable(DataSourceType.OceanBase.getVal());

    // 构建数据源信息
    private static final OceanBaseSourceDTO source = OceanBaseSourceDTO.builder()
            .url("jdbc:oceanbase://172.16.100.116:2881/sys?useUnicode=true&characterEncoding=utf-8")
            .username("root")
            .poolConfig(PoolConfig.builder().build())
            .build();

    @Test(expected = DtLoaderException.class)
    public void showPartitions() {
        List<String> list = client.showPartitions(source, "loader_test");
        Assert.assertTrue(CollectionUtils.isNotEmpty(list));
    }

    /**
     * 更改表相关参数，暂时只支持更改表注释
     */
    @Test
    public void alterTableParams() {
        Map<String,String> param = new HashMap<>();
        param.put("comment","中文");
        client.alterTableParams(source, "LOADER_TEST", param);
    }

    @Test
    public void upsertTableColumn() {
        UpsertColumnMetaDTO columnMetaDTO = new UpsertColumnMetaDTO();
        columnMetaDTO.setCommandType(CommandType.INSERT);
        columnMetaDTO.setSchema("sys");
        columnMetaDTO.setTableName("LOADER_TEST");
        columnMetaDTO.setColumnComment("comment");
        columnMetaDTO.setColumnName("age");
        columnMetaDTO.setColumnType("int");
        client.upsertTableColumn(source, columnMetaDTO);
    }

}
