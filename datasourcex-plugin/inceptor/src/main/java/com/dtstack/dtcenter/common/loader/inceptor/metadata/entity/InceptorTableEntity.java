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

package com.dtstack.dtcenter.common.loader.inceptor.metadata.entity;

import com.dtstack.dtcenter.loader.dto.metadata.entity.rdb.TableEntity;
import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * @author luming
 * @date 2022年04月15日
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class InceptorTableEntity extends TableEntity {

    /*
    | 属性名                 | 类型     | 属性中文名         | 备注                            |
    | --------------------- | ------- | --------------- | ---------------------------------|
    | tableName             |         | 表名             |                                 | 传入的表名
    | schema                |         | 数据库           |                                 | Database
    | createTime            |         | 表创建时间        |                                  | CreateTime
    | transientLastDdlTime  |         | DDL最后变更时间   |                                   | transient_lastDdlTime
    | location              |         | 存储位置         |                                  |  Location
    | tableSize             |         | 存储大小         | 优化                           | last_load_time
    | tableNumRows          | varchar | 表行数           |                              | select count(*) from 传入的表名.
    | lastAccessTime        |         | 最近同步时间      |                               | last_load_time
    | storeType             |         | 存储格式         | 值:TEXT、ORC、CSV、 Holodesk   |    [SerDe Library ]
    | tableType             | varchar | 表类型           | 所有权:外部表、托管表            | Table Type:
    | isPartition           | varchar | 是否分区         |                              |  Partition Information
    | isBucked              | varchar | 是否分桶         |                              |  Num Buckets:
    */
    /**
     * 最近一次修改时间
     */
    protected String transientLastDdlTime;
    /**
     * hdfs存储路径
     */
    protected String location;
    /**
     * 存储大小
     */
    protected String tableSize;
    /**
     * 表行数
     */
    protected String tableRows;
    /**
     * 表类型
     */
    protected String tableType;
    /**
     * 最近一次访问时间
     */
    protected String lastAccessTime;
    /**
     * hdfs存储类型
     */
    protected String storeType;
    /**
     * 是否分区
     */
    protected String isPartition;
    /**
     * isBucked
     */
    protected String isBucked;
}
