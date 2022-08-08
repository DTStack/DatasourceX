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

package com.dtstack.dtcenter.common.loader.impala.metadata.entity;

import com.dtstack.dtcenter.loader.dto.metadata.entity.rdb.TableEntity;
import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 *
 * @author luming
 * @date 2022/4/14
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class ImpalaTableEntity extends TableEntity {
    /**是否分区*/
    private boolean isPartition;

    /**最近一次访问时间*/
    private Long lastAccessTime;

    /**owner*/
    private String owner;

    /**最近一次修改时间*/
    private Long transientLastDdlTime;

    /**hdfs存储路径*/
    private String location;

    /**hdfs存储类型*/
    private String storeType;

    /**存储介质类型：hdfs/hbase/kudu*/
    private String tableType;
}
