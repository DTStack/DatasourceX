package com.dtstack.dtcenter.loader.dto.source;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.util.Map;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 19:27 2020/5/22
 * @Description：Kafka 数据源信息
 */
@Data
@ToString
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class KafkaSourceDTO implements ISourceDTO {
    /**
     * ZK 的地址
     */
    private String url;


    /**
     * kafka Brokers 的地址
     */
    private String brokerUrls;

    /**
     * 用户名
     */
    private String username;

    /**
     * 密码
     */
    private String password;

    /**
     * kerberos 配置信息
     */
    private Map<String, Object> kerberosConfig;
}
