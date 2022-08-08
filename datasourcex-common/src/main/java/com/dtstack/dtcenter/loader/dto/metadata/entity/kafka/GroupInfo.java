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

import com.dtstack.dtcenter.loader.dto.KafkaConsumerDTO;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.List;


/**
 * @author zhiyi
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class GroupInfo implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 消费者组
     */
    private String groupId;

    /**
     * 属于 topic
     */
    private String topic;

    /**
     * 分区消费信息
     */
    private List<KafkaConsumerDTO> partitionInfo;
}
