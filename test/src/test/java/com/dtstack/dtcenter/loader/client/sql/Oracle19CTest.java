package com.dtstack.dtcenter.loader.client.sql;

import com.dtstack.dtcenter.loader.cache.pool.config.PoolConfig;
import com.dtstack.dtcenter.loader.client.BaseTest;
import com.dtstack.dtcenter.loader.client.ClientCache;
import com.dtstack.dtcenter.loader.client.IClient;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.source.OracleSourceDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import org.apache.commons.collections.CollectionUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

/**
 * oracle 19C 测试
 *
 * @author ：wangchuan
 * date：Created in 上午10:11 2021/5/24
 * company: www.dtstack.com
 */
public class Oracle19CTest extends BaseTest {

    // 构造客户端
    private static final IClient client = ClientCache.getClient(DataSourceType.Oracle.getVal());

    // 数据源信息
    private static final OracleSourceDTO source = OracleSourceDTO.builder()
            .url("jdbc:oracle:thin:@//172.16.101.115:1521/LEIPDB")
            .username("sys as sysdba")
            .password("oracle")
            .pdb("PDB5")
            .poolConfig(new PoolConfig())
            .build();

    /**
     * 测试连通性测试
     */
    @Test
    public void testCon()  {
        Boolean isConnected = client.testCon(source);
        if (Boolean.FALSE.equals(isConnected)) {
            throw new DtLoaderException("connection exception");
        }
    }

    /**
     * 根据 schema获取表
     */
    @Test
    public void getTableListBySchema()  {
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().schema("TEST_WANGCHUAN_2").build();
        List<String> tableList = client.getTableListBySchema(source, queryDTO);
        Assert.assertTrue(CollectionUtils.isNotEmpty(tableList));
    }

    /**
     * 获取所有的schema
     */
    @Test
    public void getAllDatabases()  {
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().build();
        List databases = client.getAllDatabases(source, queryDTO);
        Assert.assertTrue(CollectionUtils.isNotEmpty(databases));
    }

    /**
     * 获取所有的 db
     */
    @Test
    public void getPdb()  {
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().build();
        List databases = client.getRootDatabases(source, queryDTO);
        Assert.assertTrue(CollectionUtils.isNotEmpty(databases));
    }
}
