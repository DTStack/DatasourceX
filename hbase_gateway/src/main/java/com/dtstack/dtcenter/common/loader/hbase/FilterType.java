package com.dtstack.dtcenter.common.loader.hbase;

import com.dtstack.dtcenter.common.exception.DtCenterDefException;
import com.dtstack.dtcenter.loader.enums.HbaseFilterType;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;

/**
 * 用途：将core中定义的hbase过滤器转换成hbase中的过滤器
 *
 * @author ：wangchuan
 * date：Created in 上午10:50 2020/8/25
 * company: www.dtstack.com
 */
public enum FilterType {

    PAGE_FILTER(HbaseFilterType.PAGE_FILTER.getVal()){

        @Override
        public Filter getFilter(com.dtstack.dtcenter.loader.dto.filter.Filter filter) {
            com.dtstack.dtcenter.loader.dto.filter.PageFilter pageFilter = (com.dtstack.dtcenter.loader.dto.filter.PageFilter) filter;
            return new PageFilter(pageFilter.getPageSize());
        }
    },

    SINGLE_COLUMN_VALUE_FILTER(HbaseFilterType.SINGLE_COLUMN_VALUE_FILTER.getVal()){

        @Override
        public Filter getFilter(com.dtstack.dtcenter.loader.dto.filter.Filter filter) {
            com.dtstack.dtcenter.loader.dto.filter.SingleColumnValueFilter singleColumnValueFilter = (com.dtstack.dtcenter.loader.dto.filter.SingleColumnValueFilter) filter;
            SingleColumnValueFilter columnValueFilter = new SingleColumnValueFilter(
                    singleColumnValueFilter.getColumnFamily(),
                    singleColumnValueFilter.getColumnQualifier(),
                    CompareFilter.CompareOp.valueOf(singleColumnValueFilter.getCompareOp().toString()),
                    ComparatorType.get(singleColumnValueFilter.getComparator())
            );
            columnValueFilter.setFilterIfMissing(singleColumnValueFilter.isFilterIfMissing());
            columnValueFilter.setLatestVersionOnly(singleColumnValueFilter.isLatestVersionOnly());
            columnValueFilter.setReversed(singleColumnValueFilter.isReversed());
            return columnValueFilter;
        }
    },

    ROW_FILTER(HbaseFilterType.ROW_FILTER.getVal()){
        @Override
        public Filter getFilter(com.dtstack.dtcenter.loader.dto.filter.Filter filter) {
            com.dtstack.dtcenter.loader.dto.filter.RowFilter rowFilter = (com.dtstack.dtcenter.loader.dto.filter.RowFilter) filter;
            RowFilter hbaseRowFilter = new RowFilter(
                    CompareFilter.CompareOp.valueOf(rowFilter.getCompareOp().toString()),
                    ComparatorType.get(rowFilter.getComparator()));
            hbaseRowFilter.setReversed(rowFilter.isReversed());
            return hbaseRowFilter;
        }
    };

    private Integer val;

    FilterType(Integer val) {
        this.val = val;
    }

    public static Filter get(com.dtstack.dtcenter.loader.dto.filter.Filter filter) {
        FilterType filterType = getFilterType(filter.getFilterType());
        return filterType.getFilter(filter);
    }

    public abstract Filter getFilter(com.dtstack.dtcenter.loader.dto.filter.Filter filter);

    private static FilterType getFilterType(Integer val){
        for (FilterType filterType : values()){
            if (filterType.val.equals(val)) {
                return filterType;
            }
        }
        throw new DtCenterDefException("hbase自定义查询暂时不支持该过滤器类型");
    }

}
