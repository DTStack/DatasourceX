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

package com.dtstack.dtcenter.common.loader.hivecdc.metadata.client;

import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;

import java.util.List;
import java.util.Map;

/**
 * hiveCdc执行监听任务接口
 *
 * @author luming
 * @date 2022/4/15
 */
public interface CdcJob extends Runnable {
    /**
     * 初始化
     *
     * @param sourceDTO       数据源信息
     * @param excludeDatabase 被排除的库
     * @param excludeTable    被排除的表
     * @param originalJob     初始库表数据
     */
    void init(ISourceDTO sourceDTO,
              List<String> excludeDatabase,
              List<String> excludeTable,
              List<Map<String, Object>> originalJob);

    /**
     * 执行采集
     */
    void collector();
}
