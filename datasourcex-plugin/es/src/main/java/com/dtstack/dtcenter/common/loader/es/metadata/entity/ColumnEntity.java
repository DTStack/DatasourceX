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

package com.dtstack.dtcenter.common.loader.es.metadata.entity;

import lombok.Data;

import java.io.Serializable;
import java.util.List;

/**
 * @author : luming
 * @date : 2022年04月13日
 */
@Data
public class ColumnEntity implements Serializable {

    protected static final long serialVersionUID = 1L;

    /**
     * 表示是第n个被查出来的字段
     */
    private int index;
    /**
     * 字段名称
     */
    private String name;
    /**
     * 字段类型
     */
    private String type;
    /**
     * 字段名称
     */
    private List<FieldEntity> fieldList;
}
