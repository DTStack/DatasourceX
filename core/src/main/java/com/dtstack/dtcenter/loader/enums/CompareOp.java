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
