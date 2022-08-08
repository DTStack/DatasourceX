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

/**
 * @author : luming
 * @date : 2022年04月13日
 */
@Data
public class IndexProperties implements Serializable {

    protected static final long serialVersionUID = 1L;

    /**
     * 索引存储的总容量
     */
    private String totalSize;
    /**
     * 分片数
     */
    private String shards;
    /**
     * 索引创建时间
     */
    private String createTime;
    /**
     * 副本数量
     */
    private String replicas;
    /**
     * 已删除文档数
     */
    private String docsDeleted;
    /**
     * 文档数
     */
    private String count;
    /**
     * 主分片的总容量
     */
    private String priSize;
    /**
     * green为正常，yellow表示索引不可靠（单节点），red索引不可用
     */
    private String health;
    /**
     * 索引的唯一标识
     */
    private String uuid;
    /**
     * 表明索引是否打开
     */
    private String status;
}
