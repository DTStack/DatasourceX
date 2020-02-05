package com.dtstack.dtcenter.loader.enums;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 14:15 2020/2/5
 * @Description：Redis 模式
 */
public enum RedisMode {
    /**
     * 单点
     */
    Standalone(0),

    /**
     * 主从
     */
    Master_Slave(1),

    /**
     * 哨兵
     */
    Sentinel(2),

    /**
     * 集群
     */
    Cluster(3)
    ;

    private Integer value;

    RedisMode(Integer value) {
        this.value = value;
    }
}
