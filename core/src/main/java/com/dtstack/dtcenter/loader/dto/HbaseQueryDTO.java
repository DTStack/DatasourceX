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

package com.dtstack.dtcenter.loader.dto;

import com.dtstack.dtcenter.loader.dto.filter.Filter;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.Map;

/**
 * hbase queryDTO
 *
 * @author ：wangchuan
 * date：Created in 上午10:02 2021/7/8
 * company: www.dtstack.com
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class HbaseQueryDTO {

    private String tableName;

    private List<String> columns;

    private Filter filter;

    private String startRowKey;

    private String endRowKey;

    private Long limit;

    private Map<String, ColumnType> columnTypes;

    public enum ColumnType {
        INT(),

        LONG(),

        STRING(),

        BIG_DECIMAL(),

        BOOLEAN(),

        DOUBLE(),

        FLOAT(),

        SHORT(),

        HEX()
    }
}
