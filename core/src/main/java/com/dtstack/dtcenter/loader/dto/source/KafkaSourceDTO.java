package com.dtstack.dtcenter.loader.dto.source;

import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.sql.Connection;
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
     * 数据源类型
     */
    protected Integer sourceType;

    /**
     * 是否缓存
     */
    @Builder.Default
    protected Boolean isCache = false;

    /**
     * kerberos 配置信息
     */
    private Map<String, Object> kerberosConfig;

    @Override
    public Connection getConnection() {
        throw new DtLoaderException("The method is not supported");
    }

    @Override
    public void setConnection(Connection connection) {
        throw new DtLoaderException("The method is not supported");
    }
}
