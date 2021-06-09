package com.dtstack.dtcenter.loader.dto.source;

import com.dtstack.dtcenter.loader.cache.pool.config.PoolConfig;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

import java.sql.Connection;
import java.util.Map;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 17:06 2020/5/22
 * @Description：关系型数据库 DTO
 */
@Data
@ToString
@SuperBuilder
@NoArgsConstructor
@AllArgsConstructor
public class RdbmsSourceDTO implements ISourceDTO {
    /**
     * 用户名
     */
    protected String username;

    /**
     * 密码
     */
    protected String password;

    /**
     * 数据源类型
     */
    protected Integer sourceType;

    /**
     * 地址
     */
    protected String url;

    /**
     * 库
     */
    private String schema;

    /**
     * 连接信息
     */
    private Connection connection;

    /**
     * kerberos 配置信息
     */
    private Map<String, Object> kerberosConfig;

    /**
     * 连接池配置信息，如果传入则认为开启连接池
     */
    private PoolConfig poolConfig;
}
