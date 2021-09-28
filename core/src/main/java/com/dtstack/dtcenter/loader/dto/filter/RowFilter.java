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
