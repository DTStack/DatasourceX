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

package com.dtstack.dtcenter.common.loader.doris.metadata.entity;

import com.alibaba.fastjson.annotation.JSONField;
import com.dtstack.dtcenter.loader.dto.metadata.entity.rdb.ColumnEntity;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.io.Serializable;

/**
 * Doris 字段元数据信息，包括jdbc查询结果和http查询结果
 *
 * @author luming
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class DorisColumnEntity extends ColumnEntity implements Serializable {

    private static final long serialVersionUID = 1L;

    @JSONField(name = "Field")
    private String name;

    @JSONField(name = "Type")
    private String type;

    @JSONField(name = "Null")
    private String nullAble;

    @JSONField(name = "Extra")
    private String extra;

    @JSONField(name = "Default")
    private String defaultValue;

    @JSONField(name = "Key")
    private boolean primaryKey;

    private String comment;

    private String privileges;

    private String collation;

    /**
     * 字段下标
     */
    protected Integer index;

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
