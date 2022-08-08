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

package com.dtstack.dtcenter.loader.dto.metadata;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.Map;

/**
 * metadata 采集条件 DTO
 *
 * @author ：wangchuan
 * date：Created in 下午5:38 2022/3/30
 * company: www.dtstack.com
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class MetadataCollectCondition {

    /**
     * 同步任务对应库和表集合
     */
    private List<Map<String, Object>> originalJob;

    /**
     * kafka topic
     */
    private List<String> topicList;

    /**
     * path
     */
    @Builder.Default
    private String path = "/hbase";

    /**
     * 过滤的库列表
     */
    private List<String> excludeDatabase;

    /**
     * 表过滤规则 - 正则
     */
    private List<String> excludeTable;

    /**
     * 元数据采集并行度, 默认 5, 该值不得大于 maxParallelism
     *
     * 说明：
     * <p>分片数量小于 parallelism 以为分片数量为准
     * <p>分片数量大于 parallelism 以 parallelism 为准
     * <p>分片数量大于 maxParallelism 以 maxParallelism 为准
     */
    @Builder.Default
    private Integer parallelism = 5;

    /**
     * 元数据采集最大并行度, 默认 20
     */
    @Builder.Default
    private Integer maxParallelism = 20;

    /**
     * 任务运行最大时间限制, 默认 2 小时
     */
    @Builder.Default
    private Long execTimeout = 1000 * 60 * 60 * 2L;

    /**
     * 回调批数量阈值，到达该阈值后立刻执行回调
     */
    @Builder.Default
    private Integer batchInterval = 100;
}