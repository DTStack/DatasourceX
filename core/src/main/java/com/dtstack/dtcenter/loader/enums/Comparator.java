package com.dtstack.dtcenter.loader.enums;

/**
 * -
 *
 * @author ：wangchuan
 * date：Created in 下午7:56 2020/8/24
 * company: www.dtstack.com
 */
public enum Comparator {

    BIG_DECIMAL(1),

    BINARY(2),

    BINARY_PREFIX(3),

    BIT(4),

    LONG(5),

    NULL(6),

    REGEX_STRING(7),

    SUBSTRING(8);

    private Integer val;

    Comparator(Integer val) {
        this.val = val;
    }

    public Integer getVal() {
        return val;
    }
}
