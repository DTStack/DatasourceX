package com.dtstack.dtcenter.loader.dto.comparator;

import com.dtstack.dtcenter.loader.enums.Comparator;

/**
 * -
 *
 * @author ：wangchuan
 * date：Created in 下午5:49 2020/8/24
 * company: www.dtstack.com
 */
public class LongComparator extends ByteArrayComparable{

    private Long longValue;

    public LongComparator(long value) {
        super(null);
        this.longValue = value;
    }

    public Long getLongValue() {
        return longValue;
    }

    @Override
    public Integer getComparatorType() {
        return Comparator.LONG.getVal();
    }

}
