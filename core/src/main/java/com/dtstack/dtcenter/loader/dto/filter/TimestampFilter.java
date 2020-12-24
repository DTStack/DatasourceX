package com.dtstack.dtcenter.loader.dto.filter;

import com.dtstack.dtcenter.loader.dto.comparator.ByteArrayComparable;
import com.dtstack.dtcenter.loader.enums.CompareOp;
import com.dtstack.dtcenter.loader.enums.HbaseFilterType;

/**
 * <p>时间戳过滤器
 *
 * @author ：wangchuan
 * date：Created in 下午8:18 2020/8/26
 * company: www.dtstack.com
 */
public class TimestampFilter extends Filter {

    private CompareOp compareOp;

    private Long comparator;

    @Override
    public Integer getFilterType() {
        return HbaseFilterType.TIMESTAMP_FILTER.getVal();
    }

    public TimestampFilter(CompareOp compareOp, Long comparator) {
        this.compareOp = compareOp;
        this.comparator = comparator;
    }

    public CompareOp getCompareOp() {
        return compareOp;
    }

    public Long getComparator() {
        return comparator;
    }

}
