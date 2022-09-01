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

package com.dtstack.dtcenter.loader.client;

import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;


import java.util.List;

/**
 * @author leon
 * @date 2022-06-20 15:15
 **/

public interface IRocketMq {

    /**
     * 获取 所有 Topic 信息, 包含 system topic
     *
     * @param source 数据源信息
     * @return Topic
     */
    List<String> getTopicList(ISourceDTO source);


    /**
     * 获取 所有 Topic 信息
     *
     * @param source 数据源信息
     * @param containSystemTopic 是否包含 system topic
     * @return Topic
     */
    List<String> getTopicList(ISourceDTO source, Boolean containSystemTopic );


    /**
     * 获取预览数据
     *
     * @param source   数据源信息
     * @param prevMode 预览模式
     * @return 预览数据
     */
    List<List<Object>> getPreview(ISourceDTO source, String prevMode);

}