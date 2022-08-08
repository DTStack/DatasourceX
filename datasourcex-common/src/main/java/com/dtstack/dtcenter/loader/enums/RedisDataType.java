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
