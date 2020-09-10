package com.dtstack.dtcenter.loader.dto;

import lombok.Builder;
import lombok.Data;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 16:45 2020/2/26
 * @Description：Kafka Topic 传输对象
 */
@Data
@Builder
public class KafkaTopicDTO {
    /**
     * 名称
     */
    private String topicName;

    /**
     * 分区数
     */
    private Integer partitions;

    /**
     * 复制因子
     */
    private Short replicationFactor;
}
