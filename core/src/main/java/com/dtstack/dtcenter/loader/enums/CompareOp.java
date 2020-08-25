package com.dtstack.dtcenter.loader.enums;

/**
 *
 * 类作用：hbase过滤器的操作符
 *
 * @author ：wangchuan
 * date：Created in 下午5:25 2020/8/24
 * company: www.dtstack.com
 */
public enum CompareOp {
    /** less than < */
    LESS,

    /** less than or equal to <= */
    LESS_OR_EQUAL,

    /** equals = */
    EQUAL,

    /** not equal <> */
    NOT_EQUAL,

    /** greater than or equal to >= */
    GREATER_OR_EQUAL,

    /** greater than > */
    GREATER,

    /** no operation */
    NO_OP,
}
