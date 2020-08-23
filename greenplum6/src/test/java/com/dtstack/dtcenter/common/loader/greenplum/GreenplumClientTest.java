package com.dtstack.dtcenter.common.loader.greenplum;

import com.dtstack.dtcenter.common.loader.rdbms.AbsRdbmsClient;
import org.junit.Test;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 19:45 2020/4/13
 * @Description：Greenplum 测试
 */
public class GreenplumClientTest {
    private static AbsRdbmsClient rdbsClient = new GreenplumClient();

    @Test
    public void test_issue() throws Exception {
        // 简单测试代码使用，具体全覆盖使用 core 包下面的
    }
}
