package com.dtstack.dtcenter.common.loader.hbase;

import com.dtstack.dtcenter.loader.dto.source.HbaseSourceDTO;
import org.junit.Test;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 17:07 2020/7/9
 * @Description：Hbase 客户端测试
 */
public class HbaseClientTest {
    private static HbaseClient rdbsClient = new HbaseClient();
    private String conf = "{\"hbase.zookeeper.quorum\":\"kudu1:2181,kudu2:2181,kudu3:2181\"," +
            "\"zookeeper.znode.parent\":\"/hbase\"}";
    private HbaseSourceDTO source = HbaseSourceDTO.builder().kerberosConfig(null)
            .config(conf).build();

    @Test
    public void test_issue() throws Exception {
        // 简单测试代码使用，具体全覆盖使用 core 包下面的
    }
}
