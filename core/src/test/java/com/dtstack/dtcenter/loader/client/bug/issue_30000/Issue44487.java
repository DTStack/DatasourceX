package com.dtstack.dtcenter.loader.client.bug.issue_30000;

import com.dtstack.dtcenter.loader.IDownloader;
import com.dtstack.dtcenter.loader.cache.pool.config.PoolConfig;
import com.dtstack.dtcenter.loader.client.ClientCache;
import com.dtstack.dtcenter.loader.client.IClient;
import com.dtstack.dtcenter.loader.dto.ColumnMetaDTO;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.Table;
import com.dtstack.dtcenter.loader.dto.source.HiveSourceDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @company: www.dtstack.com
 * @Author ：loader_test_1
 * @Date ：Created in 00:13 2020/2/29
 * @Description：Hive 测试
 */
@Slf4j
public class Issue44487 {

    /**
     * 构造hive客户端
     */
    private static final IClient client = ClientCache.getClient(DataSourceType.HIVE.getVal());

    /**
     * 构建数据源信息
     */
    private static final HiveSourceDTO source = HiveSourceDTO.builder()
            .url("jdbc:hive2://kudu3:10000/default")
            .schema("default")
            .defaultFS("hdfs://ns1")
            .username("admin")
            .config("{\n" +
                    "            \"dfs.ha.namenodes.ns1\" : \"nn1,nn2\",\n" +
                    "            \"dfs.namenode.rpc-address.ns1.nn2\" : \"kudu2:9000\",\n" +
                    "            \"dfs.client.failover.proxy.provider.ns1\" : \"org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider\",\n" +
                    "            \"dfs.namenode.rpc-address.ns1.nn1\" : \"kudu1:9000\",\n" +
                    "            \"dfs.nameservices\" : \"ns1\"\n" +
                    "          }")
            .poolConfig(PoolConfig.builder().build())
            .build();




    @Test
    public void getDownloader_01() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.HIVE.getVal());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("tmp_mt_htt_2_0_0").build();
        IDownloader downloader = client.getDownloader(source, queryDTO);
        Assert.assertTrue(CollectionUtils.isNotEmpty(downloader.getMetaInfo()));
        while (!downloader.reachedEnd()) {
            System.out.println(downloader.readNext());
        }
    }
}
