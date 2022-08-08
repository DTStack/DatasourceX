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

package com.dtstack.dtcenter.loader.dto.metadata.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

import java.io.Serializable;

/**
 * metadata 基类
 *
 * @author ：wangchuan
 * date：Created in 下午5:38 2022/3/30
 * company: www.dtstack.com
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@SuperBuilder
public class MetadataEntity implements Serializable {

    /**
     * 任务返回体schema
     */
    protected String schema;

    /**
     * 任务返回表名称
     */
    protected String tableName;

    /**
     * 当前对象是否同步成功
     */
    protected boolean querySuccess;

    /**
     * 失败报错信息
     */
    protected String errorMsg;

    /**
     * 操作类型
     */
    protected String operaType;
}
