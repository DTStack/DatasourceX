package com.dtstack.dtcenter.loader.dto.comparator;

import com.dtstack.dtcenter.loader.enums.Comparator;

/**
 * -
 *
 * @author ：wangchuan
 * date：Created in 下午5:49 2020/8/24
 * company: www.dtstack.com
 */
public class BitComparator extends ByteArrayComparable {

    public enum BitwiseOp {
        /** and */
        AND,
        /** or */
        OR,
        /** xor */
        XOR
    }
    protected BitwiseOp bitOperator;

    /**
     * Constructor
     * @param value value
     * @param bitOperator operator to use on the bit comparison
     */
    public BitComparator(byte[] value, BitwiseOp bitOperator) {
        super(value);
        this.bitOperator = bitOperator;
    }

    public BitwiseOp getBitOperator() {
        return bitOperator;
    }

    @Override
    public Integer getComparatorType() {
        return Comparator.BIT.getVal();
    }
}
