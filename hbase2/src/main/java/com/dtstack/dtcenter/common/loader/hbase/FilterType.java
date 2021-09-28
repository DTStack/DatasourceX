/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dtstack.dtcenter.common.loader.hbase;

import com.dtstack.dtcenter.loader.enums.HbaseFilterType;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PageFilter;
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
        throw new DtLoaderException("Hbase custom query does not support this filter type");
    }

}
