package com.dtstack.dtcenter.loader.enums;

/**
 * @Author ：qianyi
 * @Date ：Created in 10:15 2021/8/16
 * @Description：redis 数据类型
 */
public enum RedisDataType {

    /**
     * Redis Sorted Sets are, similarly to Redis Sets, non repeating collections of Strings.
     */
    STRING,

    /**
     * Redis Hashes are maps between string fields and string values.
     */
    HASH,

    /**
     * Redis Lists are simply lists of strings, sorted by insertion order.
     */
    LIST,

    /**
     * Redis Sets are an unordered collection of Strings.
     */
    SET,

    /**
     * Strings are the most basic kind of Redis value.
     */
    ZSET
}
