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

package com.dtstack.dtcenter.common.loader.doris.metadata.rest;

import com.alibaba.fastjson.annotation.JSONField;

import java.util.Map;
import java.util.StringJoiner;

/**
 * @author luming
 */
public class RowCountResponse extends DorisResponseBase {

    @JSONField(name = "data")
    private Map<String, Long> rowCount;

    public Map<String, Long> getRowCount() {
        return rowCount;
    }

    public Long getRowCount(String table) {
        return rowCount.get(table);
    }

    public void setRowCount(Map<String, Long> rowCount) {
        this.rowCount = rowCount;
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", RowCountResponse.class.getSimpleName() + "[", "]")
                .add("msg='" + msg + "'")
                .add("code=" + code)
                .add("count=" + count)
                .add("rowCount=" + rowCount)
                .toString();
    }
}
