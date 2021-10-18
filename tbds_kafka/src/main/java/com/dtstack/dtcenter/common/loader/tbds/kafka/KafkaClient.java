package com.dtstack.dtcenter.common.loader.tbds.kafka;

import com.dtstack.dtcenter.common.loader.common.nosql.AbsNoSqlClient;
import com.dtstack.dtcenter.common.loader.tbds.kafka.util.KafkaUtil;
import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;
import com.dtstack.dtcenter.loader.dto.source.TbdsKafkaSourceDTO;

/**
 * @company: www.dtstack.com
 * @Author ：wangchuan
 * @Date ：Created in 下午4:35 2020/6/2
 * @Description：Kafka 客户端 支持 Kafka 0.9、0.10、0.11、1.x版本
 */
public class KafkaClient<T> extends AbsNoSqlClient<T> {
    @Override
    public Boolean testCon(ISourceDTO sourceDTO) {
        TbdsKafkaSourceDTO kafkaSourceDTO = (TbdsKafkaSourceDTO) sourceDTO;
        return KafkaUtil.checkConnection(kafkaSourceDTO);
    }
}
