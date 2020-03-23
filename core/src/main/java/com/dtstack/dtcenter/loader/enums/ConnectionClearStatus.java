package com.dtstack.dtcenter.loader.enums;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 17:23 2020/3/23
 * @Description：连接清除状态
 */
public enum ConnectionClearStatus {
    /**
     * 关闭并清除
     */
    CLOSE(1),

    /**
     * 清除
     */
    CLEAR(2),

    /**
     * 不作处理
     */
    NORMAL(3),
    ;
    private Integer value;

    ConnectionClearStatus(Integer value) {
        this.value = value;
    }

    public Integer getValue() {
        return value;
    }
}
