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
public class DorisColumnMetaEntity implements Serializable {

    private static final Long serialVersionUID = 1L;

    @JSONField(name = "TABLE_CAT")
    private String tableCatalog;

    @JSONField(name = "TABLE_SCHEM")
    private String tableSchema;

    @JSONField(name = "TABLE_NAME")
    private String tableName;

    @JSONField(name = "COLUMN_NAME")
    private String columnName;

    @JSONField(name = "DATA_TYPE")
    private Integer dataType;

    @JSONField(name = "TYPE_NAME")
    private String typeName;

    @JSONField(name = "COLUMN_SIZE")
    private Integer columnSize;

    @JSONField(name = "BUFFER_LENGTH")
    private Long bufferLength;

    @JSONField(name = "DECIMAL_DIGITS")
    private Integer decimalDigits;

    @JSONField(name = "NUM_PREC_RADIX")
    private Integer numPrecRadix;

    @JSONField(name = "NULLABLE")
    private Integer nullable;

    @JSONField(name = "REMARKS")
    private Object remarks;

    @JSONField(name = "COLUMN_DEF")
    private Object columnDefault;

    @JSONField(name = "SQL_DATA_TYPE")
    private Object sqlDataType;

    @JSONField(name = "SQL_DATETIME_SUB")
    private Object sqlDateTimeSub;

    @JSONField(name = "CHAR_OCTET_LENGTH")
    private Object charOctetLength;

    @JSONField(name = "ORDINAL_POSITION")
    private Integer ordinalPosition;

    @JSONField(name = "IS_NULLABLE")
    private String isNullable;

    @JSONField(name = "SCOPE_CATALOG")
    private String scopeCatalog;

    @JSONField(name = "SCOPE_SCHEMA")
    private String scopeSchema;

    @JSONField(name = "SCOPE_TABLE")
    private String scopeTable;

    @JSONField(name = "SOURCE_DATA_TYPE")
    private String sourceDataType;

    @JSONField(name = "IS_AUTOINCREMENT")
    private String isAutoIncrement;

    @JSONField(name = "IS_GENERATEDCOLUMN")
    private String isGeneratedColumn;
}
