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
    @Builder.Default
    private Integer partitions = 1;

    /**
     * 复制因子
     */
    @Builder.Default
    private Short replicationFactor = 1;
}
