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

import com.dtstack.dtcenter.loader.enums.Comparator;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import org.apache.hadoop.hbase.filter.BigDecimalComparator;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.BinaryPrefixComparator;
import org.apache.hadoop.hbase.filter.BitComparator;
import org.apache.hadoop.hbase.filter.ByteArrayComparable;
import org.apache.hadoop.hbase.filter.LongComparator;
import org.apache.hadoop.hbase.filter.NullComparator;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.SubstringComparator;

/**
 * 作用：将core包下的ByteArrayComparable转换成org.apache.hadoop.hbase.filter下的ByteArrayComparable
 *
 * @author ：wangchuan
 * date：Created in 下午7:50 2020/8/24
 * company: www.dtstack.com
 */
public enum ComparatorType {

    BIG_DECIMAL(Comparator.BIG_DECIMAL.getVal()){

        @Override
        public ByteArrayComparable getComparator(com.dtstack.dtcenter.loader.dto.comparator.ByteArrayComparable comparable) {
            com.dtstack.dtcenter.loader.dto.comparator.BigDecimalComparator bigDecimalComparator = (com.dtstack.dtcenter.loader.dto.comparator.BigDecimalComparator) comparable;
            return new BigDecimalComparator(bigDecimalComparator.getBigDecimal());
        }
    },

    BINARY(Comparator.BINARY.getVal()){

        @Override
        public ByteArrayComparable getComparator(com.dtstack.dtcenter.loader.dto.comparator.ByteArrayComparable comparable) {
            return new BinaryComparator(comparable.getValue());
        }
    },

    BINARY_PREFIX(Comparator.BINARY_PREFIX.getVal()){

        @Override
        public ByteArrayComparable getComparator(com.dtstack.dtcenter.loader.dto.comparator.ByteArrayComparable comparable) {
            return new BinaryPrefixComparator(comparable.getValue());
        }
    },

    BIT(Comparator.BIT.getVal()){

        @Override
        public ByteArrayComparable getComparator(com.dtstack.dtcenter.loader.dto.comparator.ByteArrayComparable comparable) {
            com.dtstack.dtcenter.loader.dto.comparator.BitComparator bitComparator = (com.dtstack.dtcenter.loader.dto.comparator.BitComparator) comparable;
            com.dtstack.dtcenter.loader.dto.comparator.BitComparator.BitwiseOp bitOperator = bitComparator.getBitOperator();
            return new BitComparator(bitComparator.getValue(), BitComparator.BitwiseOp.valueOf(bitOperator.toString()));
        }
    },

    LONG(Comparator.LONG.getVal()){

        @Override
        public ByteArrayComparable getComparator(com.dtstack.dtcenter.loader.dto.comparator.ByteArrayComparable comparable) {
            com.dtstack.dtcenter.loader.dto.comparator.LongComparator longComparator = (com.dtstack.dtcenter.loader.dto.comparator.LongComparator) comparable;
            return new LongComparator(longComparator.getLongValue());
        }
    },

    NULL(Comparator.NULL.getVal()){

        @Override
        public ByteArrayComparable getComparator(com.dtstack.dtcenter.loader.dto.comparator.ByteArrayComparable comparable) {
            return new NullComparator();
        }
    },

    REGEX_STRING(Comparator.REGEX_STRING.getVal()){

        @Override
        public ByteArrayComparable getComparator(com.dtstack.dtcenter.loader.dto.comparator.ByteArrayComparable comparable) {
            com.dtstack.dtcenter.loader.dto.comparator.RegexStringComparator regexStringComparator = (com.dtstack.dtcenter.loader.dto.comparator.RegexStringComparator) comparable;
            return new RegexStringComparator(
                    regexStringComparator.getExpr(),
                    regexStringComparator.getFlags(),
                    RegexStringComparator.EngineType.valueOf(regexStringComparator.getEngineType().toString()));
        }
    },

    SUBSTRING(Comparator.SUBSTRING.getVal()){

        @Override
        public ByteArrayComparable getComparator(com.dtstack.dtcenter.loader.dto.comparator.ByteArrayComparable comparable) {
            com.dtstack.dtcenter.loader.dto.comparator.SubstringComparator substringComparator = (com.dtstack.dtcenter.loader.dto.comparator.SubstringComparator) comparable;
            return new SubstringComparator(substringComparator.getSubstr());
        }
    };

    private Integer val;

    ComparatorType(Integer val) {
        this.val = val;
    }

    public abstract ByteArrayComparable getComparator(com.dtstack.dtcenter.loader.dto.comparator.ByteArrayComparable comparable);

    public static ByteArrayComparable get(com.dtstack.dtcenter.loader.dto.comparator.ByteArrayComparable comparable){
        ComparatorType comparatorType = getComparatorType(comparable.getComparatorType());
        return comparatorType.getComparator(comparable);
    }

    private static ComparatorType getComparatorType(Integer val){
        for (ComparatorType comparatorType : values()){
            if (comparatorType.val.equals(val)) {
                return comparatorType;
            }
        }
        throw new DtLoaderException("Hbase custom query does not support this comparator type");
    }

}
