package com.dtstack.dtcenter.common.loader.kafka;

import com.dtstack.dtcenter.loader.dto.source.KafkaSourceDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import org.junit.Test;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 01:15 2020/2/27
 * @Description：Kafka 客户端测试
 */
public class KafkaClientTest {
    private static AbsKafka kafkaClient = new Kafka();

    @Test
    public void testCon() throws Exception {
        KafkaSourceDTO source = KafkaSourceDTO.builder()
                .brokerUrls("Kudu1:9092")
                .build();
        Boolean isConnected = kafkaClient.testCon(source);
        if (!isConnected) {
            throw new DtLoaderException("数据源连接异常");
        }
    }
}
