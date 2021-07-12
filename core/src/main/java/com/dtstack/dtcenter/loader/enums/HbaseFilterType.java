package com.dtstack.dtcenter.loader.enums;

/**
 * -
 *
 * @author ：wangchuan
 * date：Created in 上午10:52 2020/8/25
 * company: www.dtstack.com
 */
public enum HbaseFilterType {

    PAGE_FILTER(1),

    SINGLE_COLUMN_VALUE_FILTER(2),

    ROW_FILTER(3),

    TIMESTAMP_FILTER(4),

    FILTER_LIST(5);

    private final Integer val;

    HbaseFilterType(Integer val) {
        this.val = val;
    }

    public Integer getVal() {
        return val;
    }
}
