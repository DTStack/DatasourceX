package com.dtstack.dtcenter.common.loader.hive1;

import com.dtstack.dtcenter.common.loader.hive1.client.HiveClient;
import com.dtstack.dtcenter.common.loader.rdbms.AbsRdbmsClient;
import org.junit.Test;

public class HiveClientTest {
    private static AbsRdbmsClient rdbsClient = new HiveClient();

    @Test
    public void test_issue() throws Exception {
        // 简单测试代码使用，具体全覆盖使用 core 包下面的
    }
}