package com.dtstack.dtcenter.common.loader.hbase;

import com.dtstack.dtcenter.loader.enums.Comparator;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
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
            throw new DtLoaderException("hbase 1.x版本不支持该比较器");
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
        throw new DtLoaderException("hbase自定义查询暂时不支持该比较器类型");
    }

}
