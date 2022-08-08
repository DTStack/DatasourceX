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

package com.dtstack.dtcenter.loader.metadata;

import com.dtstack.dtcenter.loader.dto.metadata.MetadataCollectCondition;
import com.dtstack.dtcenter.loader.dto.metadata.entity.MetadataEntity;

import java.util.List;
import java.util.function.Consumer;

/**
 * 元数据采集 manager
 *
 * @author ：wangchuan
 * date：Created in 下午5:49 2022/4/6
 * company: www.dtstack.com
 */
public interface MetaDataCollectManager {

    /**
     * 初始化 manager 配置
     *  @param metadataCollectCondition 元数据采集条件
     * @param collectConsumer          消费者回调
     */
    void init(MetadataCollectCondition metadataCollectCondition, Consumer<List<MetadataEntity>> collectConsumer) throws Exception;

    /**
     * 是否结束
     *
     * @return 是否结束
     */
    boolean isEnd();

    /**
     * 开始采集
     */
    void start();

    /**
     * 停止采集
     */
    void stop();
}
