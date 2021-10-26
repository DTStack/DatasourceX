package com.dtstack.dtcenter.loader.dto.filter;

import com.dtstack.dtcenter.loader.enums.HbaseFilterType;

/**
 * -
 *
 * @author ：wangchuan
 * date：Created in 上午10:06 2020/8/25
 * company: www.dtstack.com
 */
@Deprecated
public class PageFilter extends Filter {

    private long pageSize;

    public long getPageSize() {
        return pageSize;
    }

    public PageFilter(long pageSize) {
        this.pageSize = pageSize;
    }

    @Override
    public Integer getFilterType() {
        return HbaseFilterType.PAGE_FILTER.getVal();
    }
}
