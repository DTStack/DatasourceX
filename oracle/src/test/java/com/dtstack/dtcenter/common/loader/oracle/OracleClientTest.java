package com.dtstack.dtcenter.common.loader.oracle;

import com.dtstack.dtcenter.common.loader.rdbms.AbsRdbmsClient;
import org.junit.Test;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 13:54 2020/1/6
 * @Description：Oracle 客户端测试
 */
public class OracleClientTest {
    private static AbsRdbmsClient rdbsClient = new OracleClient();
    
    @Test
    public void test_issue() throws Exception {
        // 简单测试代码使用，具体全覆盖使用 core 包下面的
    }
}
