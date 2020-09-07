package com.dtstack.dtcenter.loader.dto.comparator;

import com.dtstack.dtcenter.loader.enums.Comparator;

/**
 * -
 *
 * @author ：wangchuan
 * date：Created in 下午5:49 2020/8/24
 * company: www.dtstack.com
 */
public class NullComparator extends ByteArrayComparable{

    public NullComparator() {
        super(new byte[0]);
    }

    @Override
    public Integer getComparatorType() {
        return Comparator.NULL.getVal();
    }
}
