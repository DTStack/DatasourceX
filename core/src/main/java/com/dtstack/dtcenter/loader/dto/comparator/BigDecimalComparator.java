package com.dtstack.dtcenter.loader.dto.comparator;

import com.dtstack.dtcenter.loader.enums.Comparator;

import java.math.BigDecimal;

/**
 *
 *
 * @author ：wangchuan
 * date：Created in 下午5:48 2020/8/24
 * company: www.dtstack.com
 */
public class BigDecimalComparator extends ByteArrayComparable {

    private BigDecimal bigDecimal;

    public BigDecimalComparator(BigDecimal value) {
        super(null);
        this.bigDecimal = value;
    }

    public BigDecimal getBigDecimal() {
        return bigDecimal;
    }

    @Override
    public Integer getComparatorType() {
        return Comparator.BIG_DECIMAL.getVal();
    }
}
