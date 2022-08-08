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
import lombok.Data;

import java.io.Serializable;

/**
 * @author luming
 */
@Data
public class DorisIndexEntity implements Serializable {

    private static final long serialVersionUID = 1L;

    @JSONField(name = "Table")
    private String table;

    @JSONField(name = "Non_unique")
    private boolean nonUnique;

    @JSONField(name = "Key_name")
    private String indexName;

    @JSONField(name = "Seq_in_index")
    private String seqInIndex;

    @JSONField(name = "Column_name")
    private String columnName;

    @JSONField(name = "Collation")
    private String collation;

    @JSONField(name = "Cardinality")
    private String cardinality;

    @JSONField(name = "Sub_part")
    private String subPart;

    @JSONField(name = "Packed")
    private boolean packed;

    @JSONField(name = "Null")
    private boolean isNull;

    @JSONField(name = "Index_type")
    private String indexType;

    @JSONField(name = "Comment")
    private String indexComment;
}
