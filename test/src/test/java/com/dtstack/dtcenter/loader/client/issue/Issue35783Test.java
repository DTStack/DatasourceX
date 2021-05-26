package com.dtstack.dtcenter.loader.client.issue;

import com.dtstack.dtcenter.loader.cache.pool.config.PoolConfig;
import com.dtstack.dtcenter.loader.client.BaseTest;
import com.dtstack.dtcenter.loader.client.ClientCache;
import com.dtstack.dtcenter.loader.client.IClient;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.source.Mysql5SourceDTO;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

/**
 *
 * bug描述：mysql 等 RDBMS 查询会出现最大条数无效的问题
 *
 * bug链接：http://zenpms.dtstack.cn/zentao/bug-view-35783.html
 *
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 09:42 2021/03/23
 */
public class Issue35783Test extends BaseTest {
    // 获取数据源 client
    private static final IClient client = ClientCache.getClient(DataSourceType.MySQL.getVal());

    // 构建数据源信息
    private static final Mysql5SourceDTO source = Mysql5SourceDTO.builder()
            .url("jdbc:mysql://172.16.100.186:3306/dev")
            .username("dev")
            .password("Abc12345")
            .poolConfig(PoolConfig.builder().build())
            .build();

    /**
     * 数据预处理
     */
    @BeforeClass
    public static void beforeClass() {
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("drop table if exists LOADER_TEST_35783").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("create table LOADER_TEST_35783 (id int)").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("insert into LOADER_TEST_35783 values (1), (2), (3), (4), (5), (6), (7), (8), (9), (10)").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
    }

    /**
     * 获取连接测试
     */
    @Test
    public void testIssue() throws Exception{
        List list = client.executeQuery(source, SqlQueryDTO.builder().sql("select * from LOADER_TEST_35783").limit(5).build());
        Assert.assertEquals(5, list.size());
    }
}
