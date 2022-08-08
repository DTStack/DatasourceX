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

package com.dtstack.dtcenter.common.loader.metadata.core;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * metadata 基础切片
 *
 * @author ：wangchuan
 * date：Created in 上午11:56 2022/4/6
 * company: www.dtstack.com
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class MetadataBaseCollectSplit {

    /**
     * 分片对应的库名，分片规则为一个库对应一个分片
     */
    protected String dbName;

    /**
     * 为了兼容查询table或者schema table结构
     */
    protected List<Object> tableList;

    /**
     * kafka topic
     */
    protected List<String> topicList;

    /**
     * countDownLatch 当前分片处理完毕后 -1
     */
    protected CountDownLatch countDownLatch;
}
