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

package com.dtstack.dtcenter.common.loader.hivecdc.metadata.entity;

import lombok.Data;

import java.io.Serializable;

/**
 * 表层级的元数据
 *
 * @author luming
 */
@Data
public class TableEntity implements Serializable {

    protected static final long serialVersionUID = 1L;

    /**
     * 表名称
     */
    protected String tableName;

    /**
     * 创建时间
     */
    protected String createTime;

    /**
     * 表的描述
     */
    protected String comment;

    /**
     * 表大小
     */
    protected Long totalSize;

    /**
     * 表数据量数目
     */
    protected Long rows;
}
