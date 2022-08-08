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

package com.dtstack.dtcenter.common.loader.hbase.restful.cons;

import com.dtstack.dtcenter.loader.exception.DtLoaderException;

import java.util.Objects;

/**
 * 过滤器类型枚举类
 *
 * @author luming
 * @date 2022/5/9
 */
public enum FilterType {
    PAGE_FILTER(1, "PageFilter"),

    SINGLE_COLUMN_VALUE_FILTER(2, "SingleColumnValueFilter"),

    ROW_FILTER(3, "RowFilter"),

    TIMESTAMP_FILTER(4, "TimestampsFilter"),

    QUALIFIER_FILTER(6, "QualifierFilter");

    private final Integer code;
    private final String name;

    FilterType(Integer code, String name) {
        this.code = code;
        this.name = name;
    }

    public Integer getCode() {
        return code;
    }

    public String getName() {
        return name;
    }

    /**
     * 根据HbaseFilterType的值获取hbase restful需要的filter name
     *
     * @param code code
     * @return filter name
     */
    public static String getType(Integer code) {
        for (FilterType type : FilterType.values()) {
            if (Objects.equals(type.getCode(), code)) {
                return type.getName();
            }
        }
        throw new DtLoaderException("Hbase restful don't support this kind of filter");
    }
}
