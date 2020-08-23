package com.dtstack.dtcenter.common.loader.hbase;

import com.dtstack.dtcenter.loader.dto.source.HbaseSourceDTO;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 21:42 2020/2/27
 * @Description：Hbase 客户端测试
 */
@Slf4j
public class HbaseClientTest {
    private static HbaseClient rdbsClient = new HbaseClient();
    private String conf = "{\"hbase.zookeeper.quorum\":\"172.16.10.104:2181,172.16.10.224:2181,172.16.10.252:2181\"," +
            "\"zookeeper.znode.parent\":\"/hbase\"}";
    private HbaseSourceDTO source = HbaseSourceDTO.builder().kerberosConfig(null)
            .config(conf).build();

    private HbaseSourceDTO source2 = HbaseSourceDTO.builder().kerberosConfig(null).url("172.16.10.104:2181,172.16.10" +
            ".224:2181,172.16.10.252:2181")
            .path("/hbase").build();

    @Test
    public void test_issue() throws Exception {
        // 简单测试代码使用，具体全覆盖使用 core 包下面的
    }
}
