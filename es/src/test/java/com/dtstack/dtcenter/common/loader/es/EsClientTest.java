package com.dtstack.dtcenter.common.loader.es;

import com.dtstack.dtcenter.loader.dto.source.ESSourceDTO;
import org.junit.Test;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 21:57 2020/2/27
 * @Description：ES 客户端测试
 */
public class EsClientTest {
    private static EsClient rdbsClient = new EsClient();
    ESSourceDTO source = ESSourceDTO.builder()
            .url("kudu5:9200")
            .username("elastic")
            .others("abc123")
            .build();

    @Test
    public void test_issue() throws Exception {
        // 简单测试代码使用，具体全覆盖使用 core 包下面的
    }
}
