package com.dtstack.dtcenter.loader.dto.filter;

import com.dtstack.dtcenter.loader.dto.comparator.ByteArrayComparable;
import com.dtstack.dtcenter.loader.enums.CompareOp;
import com.dtstack.dtcenter.loader.enums.HbaseFilterType;

/**
 *
 * @author ：wangchuan
 * date：Created in 下午8:18 2020/8/26
 * company: www.dtstack.com
 */
public class RowFilter extends Filter {

    private CompareOp compareOp;

    private ByteArrayComparable comparator;

    private boolean reversed = false;

    @Override
    public Integer getFilterType() {
        return HbaseFilterType.ROW_FILTER.getVal();
    }

    public RowFilter(CompareOp compareOp, ByteArrayComparable comparator) {
        this.compareOp = compareOp;
        this.comparator = comparator;
    }

    public CompareOp getCompareOp() {
        return compareOp;
    }

    public ByteArrayComparable getComparator() {
        return comparator;
    }

    public boolean isReversed() {
        return reversed;
    }

    public void setReversed(boolean reversed) {
        this.reversed = reversed;
    }
}
