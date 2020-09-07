package com.dtstack.dtcenter.loader.dto.comparator;

/**
 * -
 *
 * @author ：wangchuan
 * date：Created in 下午5:50 2020/8/24
 * company: www.dtstack.com
 */
public abstract class ByteArrayComparable {

    byte[] value;

    public ByteArrayComparable(byte[] value) {
        this.value = value;
    }

    public byte[] getValue() {
        return value;
    }

    public abstract Integer getComparatorType();

}
