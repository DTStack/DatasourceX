package com.dtstack.dtcenter.loader.cache.pool.config;

import lombok.Builder;
import lombok.Data;

import java.io.Serializable;

import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 14:24 2020/7/23
 * @Description：连接池配置信息
 */
@Data
public class PoolConfig implements Serializable {
    /**
     * 等待连接池分配连接的最大时长（毫秒）
     * 超过这个时长还没可用的连接则发生SQLException
     */
    private Long connectionTimeout = SECONDS.toMillis(3);

    /**
     * 控制允许连接在池中闲置的最长时间
     * 此设置仅适用于 minimumIdle 设置为小于 maximumPoolSize 的情况
     */
    private Long idleTimeout = MINUTES.toMillis(10);

    /**
     * 一个连接的生命时长（毫秒），超时而且没被使用则被释放（retired）
     * 建议设置比数据库超时时长少30秒
     */
    private Long maxLifetime = MINUTES.toMillis(30);

    /**
     * 连接池中允许的最大连接数(包括空闲和正在使用的连接)
     */
    private Integer maximumPoolSize = 10;

    /**
     * 池中维护的最小空闲连接数
     * 小于 0 则会重置为最大连接数
     */
    private Integer minimumIdle = 5;

    /**
     * 设置连接只读
     */
    private Boolean readOnly = false;

    /**
     * 申请连接的时候检测，对es、odps有效，如果空闲时间大于timeBetweenEvictionRunsMillis，执行validationQuery检测连接是否有效。
     */
    private Boolean testWhileIdle = true;

    /**
     * 初始连接池大小，对es、odps有效
     */
    private Integer initialSize = 5;

    /**
     * 配置间隔多久才进行一次检测，检测需要关闭的空闲连接，单位是毫秒。 对es、odps有效
     */
    public Long timeBetweenEvictionRunsMillis = SECONDS.toMillis(30);

    /**
     * 配置一个连接在池中最小生存的时间，单位是毫秒，默认五分钟 对es、odps有效
     */
    public Long minEvictableIdleTimeMillis = MINUTES.toMillis(30);

    public Integer getMinimumIdle() {
        return minimumIdle < 0 || minimumIdle > getMaximumPoolSize() ? getMaximumPoolSize() : minimumIdle;
    }
}
