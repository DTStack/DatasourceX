package com.dtstack.dtcenter.loader.client.sql;

import com.dtstack.dtcenter.loader.client.BaseTest;
import com.dtstack.dtcenter.loader.client.ClientCache;
import com.dtstack.dtcenter.loader.client.IClient;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.source.KylinRestfulSourceDTO;
import com.dtstack.dtcenter.loader.dto.source.KylinSourceDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Connection;
import java.util.List;
import java.util.Map;

public class KylinRestfulTest extends BaseTest {

    // 构建client
    private static final IClient client = ClientCache.getClient(DataSourceType.KylinRestful.getVal());

    // 构建数据源信息
    private static final KylinRestfulSourceDTO source = KylinRestfulSourceDTO.builder()
            .project("learn_kylin")
            .url("172.16.101.17:7070")
            .userName("ADMIN")
            .password("KYLIN")
            .build();


    @Test
    public void testCon() {
        Boolean isConnected = client.testCon(source);
        if (Boolean.FALSE.equals(isConnected)) {
            throw new DtLoaderException("connection exception");
        }
    }


    @Test
    public void getAllDbList() {
        List<String> dbList = client.getAllDatabases(source, SqlQueryDTO.builder().build());
        Assert.assertTrue(CollectionUtils.isNotEmpty(dbList));
    }

    @Test
    public void getTableList() {
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().schema("default").build();
        List<String> tableList = client.getTableListBySchema(source, queryDTO);
        Assert.assertTrue(CollectionUtils.isNotEmpty(tableList));
    }
}
