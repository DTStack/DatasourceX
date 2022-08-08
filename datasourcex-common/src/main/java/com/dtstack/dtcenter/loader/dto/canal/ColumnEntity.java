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

package com.dtstack.dtcenter.loader.dto.canal;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 监听字段信息
 *
 * @author by zhiyi
 * @date 2022/3/17 10:13 上午
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ColumnEntity {

    /**
     * 字段名
     */
    private String name;

    /**
     * 变更前值
     */
    private String beforeValue;

    /**
     * 变更后值
     */
    private String afterValue;

    /**
     * 数据类型
     */
    private String mysqlType;

    /**
     * 值是否发生变更
     */
    private Boolean updated;
}
