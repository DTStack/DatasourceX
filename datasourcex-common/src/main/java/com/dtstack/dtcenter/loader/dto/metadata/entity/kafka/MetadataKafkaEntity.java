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

package com.dtstack.dtcenter.loader.dto.metadata.entity.kafka;

import com.dtstack.dtcenter.loader.dto.metadata.entity.MetadataEntity;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

import java.io.Serializable;
import java.util.List;

/**
 * @author zhiyi
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@SuperBuilder
public class MetadataKafkaEntity extends MetadataEntity implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * topic name
     */
    private String topicName;
    /**
     * 分区数
     */
    private Integer partitions;
    /**
     * 副本数
     */
    private Integer replicationFactor;

    /**
     * 时间戳
     */
    private Long timeStamp;

    /**
     * 消费组信息
     */
    private List<GroupInfo> groupInfo;

    /**
     * 是否查询成功
     */
    private boolean querySuccess;

    /**
     * 错误信息
     */
    private String errorMsg;
}
