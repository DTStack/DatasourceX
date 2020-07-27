package com.dtstack.dtcenter.loader.cache.cp;

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
@Builder
public class CpConfig implements Serializable {
    /**
     * 等待连接池分配连接的最大时长（毫秒）
     * 超过这个时长还没可用的连接则发生SQLException
     */
    @Builder.Default
    private Long connectionTimeout = SECONDS.toMillis(30);

    /**
     * 控制允许连接在池中闲置的最长时间
     * 此设置仅适用于 minimumIdle 设置为小于 maximumPoolSize 的情况
     */
    @Builder.Default
    private Long idleTimeout = MINUTES.toMillis(10);

    /**
     * 一个连接的生命时长（毫秒），超时而且没被使用则被释放（retired）
     * 建议设置比数据库超时时长少30秒
     */
    @Builder.Default
    private Long maxLifetime = MINUTES.toMillis(30);

    /**
     * 连接池中允许的最大连接数(包括空闲和正在使用的连接)
     */
    @Builder.Default
    private Integer maximumPoolSize = 10;

    /**
     * 池中维护的最小空闲连接数
     * 小于 0 则会重置为最大连接数
     */
    @Builder.Default
    private Integer minimumIdle = -1;

    /**
     * 设置连接只读
     */
    @Builder.Default
    private Boolean readOnly = false;

    public Integer getMinimumIdle() {
        return minimumIdle < 0 || minimumIdle > getMaximumPoolSize() ? getMaximumPoolSize() : minimumIdle;
    }
}
