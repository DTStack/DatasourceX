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

package com.dtstack.dtcenter.loader.dto.metadata.entity.rdb;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

import java.io.Serializable;

/**
 * rdbms metadata entity
 *
 * @author ：wangchuan
 * date：Created in 上午9:22 2022/4/6
 * company: www.dtstack.com
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@SuperBuilder
public class ColumnEntity implements Serializable {

    /**
     * 是否是主键
     */
    private String primaryKey;

    /**
     * 字段默认值
     */
    protected String defaultValue;

    /**
     * 字段类型
     */
    protected String type;

    /**
     * 字段名称
     */
    protected String name;

    /**
     * 字段描述
     */
    protected String comment;

    /**
     * 字段下标
     */
    protected Integer index;

    /**
     * 字段是否可以为空
     */
    protected String nullAble;

    /**
     * 字段长度
     */
    protected Integer length;

    /**
     * 小数点长度
     */
    protected Integer digital;

    /**
     * 字段精度
     */
    protected Integer precision;
}
