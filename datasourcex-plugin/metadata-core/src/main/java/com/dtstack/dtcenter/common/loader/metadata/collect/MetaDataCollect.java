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

package com.dtstack.dtcenter.common.loader.metadata.collect;

import com.dtstack.dtcenter.common.loader.metadata.core.MetadataBaseCollectSplit;
import com.dtstack.dtcenter.loader.dto.metadata.MetadataCollectCondition;
import com.dtstack.dtcenter.loader.dto.metadata.entity.MetadataEntity;
import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;

import java.io.IOException;
import java.util.Queue;

/**
 * collect
 *
 * @author ：wangchuan
 * date：Created in 下午5:40 2022/4/6
 * company: www.dtstack.com
 */
public interface MetaDataCollect extends AutoCloseable, Runnable {

    /**
     * 初始化方法
     *
     * @param sourceDTO                数据源信息
     * @param metadataCollectCondition 元数据采集条件
     * @param metadataBaseCollectSplit 分片
     * @param metadataEntities         存放采集结果的queue
     * @throws IOException 异常信息
     */
    void init(ISourceDTO sourceDTO,
              MetadataCollectCondition metadataCollectCondition,
              MetadataBaseCollectSplit metadataBaseCollectSplit,
              Queue<MetadataEntity> metadataEntities) throws Exception;

    /**
     * 读取下一条元数据
     *
     * @return 元数据信息
     */
    MetadataEntity readNext();

    /**
     * 判断是否读取完毕
     *
     * @return 是否读取完毕
     */
    boolean reachEnd();

    /**
     * 关闭资源
     */
    void close();
}
