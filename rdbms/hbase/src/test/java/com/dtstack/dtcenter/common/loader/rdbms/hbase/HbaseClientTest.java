package com.dtstack.dtcenter.common.loader.rdbms.hbase;

import com.dtstack.dtcenter.common.exception.DtCenterDefException;
import com.dtstack.dtcenter.common.loader.rdbms.common.AbsRdbmsClient;
import com.dtstack.dtcenter.loader.dto.SourceDTO;
import org.junit.Test;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 21:42 2020/2/27
 * @Description：Hbase 客户端测试
 */
public class HbaseClientTest {
    private static AbsRdbmsClient rdbsClient = new HbaseClient();

    @Test
    public void getConnFactory() throws Exception {
        SourceDTO source = SourceDTO.builder()
                .url("172-16-8-193:2181")
                .path("/hbase")
                .others("{\n" +
                        "    \"zookeeper.znode.parent\": \"/hbase\"\n" +
                        "}")
                .build();
        Boolean isConnected = rdbsClient.testCon(source);
        if (!isConnected) {
            throw new DtCenterDefException("数据源连接异常");
        }
    }
}
