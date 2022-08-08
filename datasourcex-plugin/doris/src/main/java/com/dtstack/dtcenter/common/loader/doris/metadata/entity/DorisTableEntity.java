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
import com.dtstack.dtcenter.loader.dto.metadata.entity.rdb.TableEntity;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.io.Serializable;

/**
 * 当前表对应的元数据信息
 *
 * @author luming
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class DorisTableEntity extends TableEntity implements Serializable {

    private static final long serialVersionUID = 1L;

    @JSONField(name = "TABLE_CATALOG")
    private String tableCatalog;

    @JSONField(name = "TABLE_SCHEMA")
    private String tableSchema;

    @JSONField(name = "TABLE_NAME")
    private String tableName;

    @JSONField(name = "TABLE_TYPE")
    private String tableType;

    @JSONField(name = "ENGINE")
    private String engine;

    @JSONField(name = "VERSION")
    private String version;

    @JSONField(name = "ROW_FORMAT")
    private String rowFormat;

    @JSONField(name = "TABLE_ROWS")
    private Integer tableRows;

    @JSONField(name = "AVG_ROW_LENGTH")
    private Integer avgRowLength;

    @JSONField(name = "DATA_LENGTH")
    private Integer dataLength;

    @JSONField(name = "MAX_DATA_LENGTH")
    private Integer maxDataLength;

    @JSONField(name = "INDEX_LENGTH")
    private Integer indexLength;

    @JSONField(name = "DATA_FREE")
    private Boolean dataFree;

    @JSONField(name = "AUTO_INCREMENT")
    private Boolean autoIncrement;

    @JSONField(name = "CREATE_TIME")
    private Long createTime;

    @JSONField(name = "UPDATE_TIME")
    private String lastModifyTime;

    @JSONField(name = "CHECK_TIME")
    private String checkTime;

    @JSONField(name = "TABLE_COLLATION")
    private String tableCollation;

    @JSONField(name = "CHECKSUM")
    private String checkSum;

    @JSONField(name = "CREATE_OPTIONS")
    private String createOptions;

    @JSONField(name = "TABLE_COMMENT")
    private String comment;
}
