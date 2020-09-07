package com.dtstack.dtcenter.loader.dto.comparator;

import com.dtstack.dtcenter.loader.enums.Comparator;

/**
 * -
 *
 * @author ：wangchuan
 * date：Created in 下午5:50 2020/8/24
 * company: www.dtstack.com
 */
public class SubstringComparator extends ByteArrayComparable {

    private String substr;

    /**
     * Constructor
     * @param substr the substring
     */
    public SubstringComparator(String substr) {
        super(null);
        this.substr = substr;
    }

    @Override
    public Integer getComparatorType() {
        return Comparator.SUBSTRING.getVal();
    }

    public String getSubstr() {
        return substr;
    }

}
