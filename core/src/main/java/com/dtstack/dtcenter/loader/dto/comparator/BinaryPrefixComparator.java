package com.dtstack.dtcenter.loader.dto.comparator;

import com.dtstack.dtcenter.loader.enums.Comparator;

/**
 *
 *
 * @author ：wangchuan
 * date：Created in 下午5:48 2020/8/24
 * company: www.dtstack.com
 */
public class BinaryPrefixComparator extends ByteArrayComparable {

    /**
     * Constructor
     * @param value value
     */
    public BinaryPrefixComparator(byte[] value) {
        super(value);
    }

    @Override
    public Integer getComparatorType() {
        return Comparator.BINARY_PREFIX.getVal();
    }
}
