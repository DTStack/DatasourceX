package com.dtstack.dtcenter.loader.client.sql;

import com.dtstack.dtcenter.loader.cache.pool.config.PoolConfig;
import com.dtstack.dtcenter.loader.client.ClientCache;
import com.dtstack.dtcenter.loader.client.IClient;
import com.dtstack.dtcenter.loader.dto.ColumnMetaDTO;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.source.PrestoSourceDTO;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * presto inceptor 测试
 *
 * @author ：qianyi
 * date：Created in 上午9:50 2021/3/23
 * company: www.dtstack.com
 */
@Ignore
public class PrestoInceptorTest {

    // 获取数据源 client
    private static final IClient client = ClientCache.getClient(DataSourceType.Presto.getVal());

    // 构建数据源信息, 测试库
    private static final PrestoSourceDTO source = PrestoSourceDTO.builder()
            .url("jdbc:presto://172.16.23.196:8080/inceptorsql")
            .username("root")
            .poolConfig(PoolConfig.builder().build())
            .build();
    /**
     * 获取连接测试
     */
    @Test
    public void getCon() throws Exception {
        Connection connection = client.getCon(source);
        Assert.assertNotNull(connection);
        connection.close();
    }

    @Test
    public void getTableList() {
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().schema("default").build();
        List<String> tableList = client.getTableListBySchema(source, queryDTO);
        tableList.forEach(table-> {
            client.getColumnMetaData(source, SqlQueryDTO.builder().schema("default").tableName(table).build());
        });
    }

    @Test
    public void getColumnMetaData() {
        client.executeQuery(source, SqlQueryDTO.builder().sql("DESCRIBE default.dl_user").build() );
        client.getColumnMetaData(source, SqlQueryDTO.builder().schema("default").tableName("dl_user").build());
    }
}
