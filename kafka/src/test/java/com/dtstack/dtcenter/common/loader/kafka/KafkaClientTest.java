package com.dtstack.dtcenter.common.loader.kafka;

import com.dtstack.dtcenter.common.exception.DtCenterDefException;
import com.dtstack.dtcenter.loader.dto.SourceDTO;
import org.junit.Test;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 01:15 2020/2/27
 * @Description：Kafka 客户端测试
 */
public class KafkaClientTest {
    private static AbsKafkasClient kafkaClient = new KafkaClient();

    @Test
    public void testCon() throws Exception {
        SourceDTO source = SourceDTO.builder()
                .brokerUrls("Kudu1:9092")
                .build();
        Boolean isConnected = kafkaClient.testCon(source);
        if (!isConnected) {
            throw new DtCenterDefException("数据源连接异常");
        }
    }
}
