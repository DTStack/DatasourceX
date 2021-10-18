package com.dtstack.dtcenter.loader.client.sql;

import com.dtstack.dtcenter.loader.cache.pool.config.PoolConfig;
import com.dtstack.dtcenter.loader.client.BaseTest;
import com.dtstack.dtcenter.loader.client.ClientCache;
import com.dtstack.dtcenter.loader.client.IHbase;
import com.dtstack.dtcenter.loader.dto.HbaseQueryDTO;
import com.dtstack.dtcenter.loader.dto.comparator.BinaryComparator;
import com.dtstack.dtcenter.loader.dto.filter.FilterList;
import com.dtstack.dtcenter.loader.dto.filter.RowFilter;
import com.dtstack.dtcenter.loader.dto.filter.SingleColumnValueFilter;
import com.dtstack.dtcenter.loader.dto.source.HbaseSourceDTO;
import com.dtstack.dtcenter.loader.enums.CompareOp;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.collections.CollectionUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * hbase新客户端无kerberos认证测试
 *
 * @author ：wangchuan
 * date：Created in 9:38 上午 2020/12/2
 * company: www.dtstack.com
 */
public class TbdsHbaseClientTest extends BaseTest {

    // 构建hbase client
    private static final IHbase HBASE_CLIENT = ClientCache.getHbase(DataSourceType.TBDS_HBASE.getVal());

    // 构建数据源信息
    private static final HbaseSourceDTO source = HbaseSourceDTO.builder()
            .url("172.16.100.175:2181,172.16.101.196:2181,172.16.101.227:2181")
            .path("/hbase")
            .build();
}
