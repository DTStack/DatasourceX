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

package com.dtstack.dtcenter.common.loader.hbase.metadata.entity;

import com.dtstack.dtcenter.loader.dto.metadata.entity.MetadataEntity;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.List;

/**
 * @author luming
 * @date 2022年04月13日
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class MetadataHbaseEntity extends MetadataEntity {
    /**
     * hbase 表的参数
     */
    private HbaseTableEntity tableProperties;

    /**
     * hbase 字段的参数
     */
    private List<HbaseColumnEntity> columns;

    /**
     * hbase 表的名称
     */
    private String tableName;
}
