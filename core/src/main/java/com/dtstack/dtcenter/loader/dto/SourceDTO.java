package com.dtstack.dtcenter.loader.dto;

import com.dtstack.dtcenter.common.enums.DataSourceType;
import com.dtstack.dtcenter.loader.enums.RedisMode;
import lombok.Builder;
import lombok.Data;
import org.apache.commons.lang.StringUtils;

import java.sql.Connection;
import java.util.Map;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 16:33 2020/1/17
 * @Description：数据库连接
 */
@Data
@Builder
public class SourceDTO {

    /**
     * 数据源类型
     */
    private DataSourceType sourceType;

    /**
     * URL 地址，如果为 master slave 的则为所有的地址
     * 如果为 Kafka 则为 ZK 的地址
     * 如果为 HBASE 则为 HBASE 集群地址
     */
    private String url;

    /**
     * 如果为 master slave 的则为 master 的地址
     */
    private String master;

    /**
     * kafka Brokers 的地址
     */
    private String brokerUrls;

    /**
     * 端口号
     */
    private String hostPort;

    /**
     * 模式即 DBName
     */
    private String schema;

    /**
     * 用户名
     */
    private String username;

    /**
     * 密码
     */
    private String password;

    /**
     * Hadoop defaultFS
     */
    private String defaultFS;

    /**
     * Hadoop/ Hbase 配置信息
     */
    private String config;

    /**
     * 其他配置信息
     */
    private String others;

    /**
     * 目录
     * Hbase 根目录
     * FTP rsa 路径
     */
    private String path;

    /**
     * 连接模式
     */
    private String connectMode;

    /**
     * 协议
     */
    private String protocol;

    /**
     * 认证
     */
    private String auth;

    /**
     * Redis 部署模式
     */
    private RedisMode redisMode;

    /**
     * 数据库持续性连接
     */
    private Connection connection;

    /**
     * kerberos 配置信息
     */
    private Map<String, Object> kerberosConfig;
}
