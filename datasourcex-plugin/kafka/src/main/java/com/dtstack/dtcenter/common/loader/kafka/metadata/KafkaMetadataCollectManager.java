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

package com.dtstack.dtcenter.common.loader.kafka.metadata;

import com.dtstack.dtcenter.common.loader.kafka.util.KafkaUtil;
import com.dtstack.dtcenter.common.loader.metadata.Manager.AbstractMetaDataCollectManager;
import com.dtstack.dtcenter.common.loader.metadata.collect.MetaDataCollect;
import com.dtstack.dtcenter.common.loader.metadata.core.MetadataBaseCollectSplit;
import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;
import com.dtstack.dtcenter.loader.dto.source.KafkaSourceDTO;
import org.apache.commons.collections.CollectionUtils;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * kafka 元数据拾取管理器
 *
 * @author by zhiyi
 * @date 2022/4/8 1:52 下午
 */
public class KafkaMetadataCollectManager extends AbstractMetaDataCollectManager {

    public KafkaMetadataCollectManager(ISourceDTO sourceDTO) {
        super(sourceDTO);
    }

    @Override
    protected Class<? extends MetaDataCollect> getMetadataCollectClass() {
        return KafkaMetadataCollect.class;
    }

    @Override
    protected List<MetadataBaseCollectSplit> createCollectSplits() {
        List<String> topicList = metadataCollectCondition.getTopicList();
        //为空则代表获取全部的topic信息
        if (CollectionUtils.isEmpty(topicList)) {
            topicList = KafkaUtil.getTopicList((KafkaSourceDTO) sourceDTO);
        }
        return Collections.singletonList(MetadataBaseCollectSplit.builder().topicList(topicList).build());
    }
}
