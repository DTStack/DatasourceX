package com.dtstack.dtcenter.loader.dto.filter;

import com.dtstack.dtcenter.loader.dto.comparator.BinaryComparator;
import com.dtstack.dtcenter.loader.dto.comparator.ByteArrayComparable;
import com.dtstack.dtcenter.loader.enums.CompareOp;
import com.dtstack.dtcenter.loader.enums.HbaseFilterType;

/**
 * <p>用于hbase自定义查询</p>
 *
 * @author ：wangchuan
 * date：Created in 下午5:21 2020/8/24
 * company: www.dtstack.com
 */
public class SingleColumnValueFilter extends Filter {

    private byte [] columnFamily;

    private byte [] columnQualifier;

    private CompareOp compareOp;

    private ByteArrayComparable comparator;

    private boolean foundColumn = false;

    private boolean matchedColumn = false;

    private boolean filterIfMissing = false;

    private boolean latestVersionOnly = true;

    private boolean reversed = false;

    public SingleColumnValueFilter(final byte [] family, final byte [] qualifier,
                                   final CompareOp compareOp, final byte[] value) {
        this(family, qualifier, compareOp, new BinaryComparator(value));
    }

    public SingleColumnValueFilter(final byte [] family, final byte [] qualifier,
                                   final CompareOp compareOp, final ByteArrayComparable comparator) {
        this.columnFamily = family;
        this.columnQualifier = qualifier;
        this.compareOp = compareOp;
        this.comparator = comparator;
    }

    @Override
    public Integer getFilterType() {
        return HbaseFilterType.SINGLE_COLUMN_VALUE_FILTER.getVal();
    }

    public byte[] getColumnFamily() {
        return columnFamily;
    }

    public byte[] getColumnQualifier() {
        return columnQualifier;
    }

    public CompareOp getCompareOp() {
        return compareOp;
    }

    public ByteArrayComparable getComparator() {
        return comparator;
    }

    public boolean isFilterIfMissing() {
        return filterIfMissing;
    }

    public void setFilterIfMissing(boolean filterIfMissing) {
        this.filterIfMissing = filterIfMissing;
    }

    public boolean isLatestVersionOnly() {
        return latestVersionOnly;
    }

    public void setLatestVersionOnly(boolean latestVersionOnly) {
        this.latestVersionOnly = latestVersionOnly;
    }

    public boolean isReversed() {
        return reversed;
    }

    public void setReversed(boolean reversed) {
        this.reversed = reversed;
    }
}
