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

package com.dtstack.dtcenter.common.loader.hivecdc.metadata.collect;

import com.dtstack.dtcenter.common.loader.metadata.Manager.AbstractMetaDataCollectManager;
import com.dtstack.dtcenter.common.loader.metadata.collect.MetaDataCollect;
import com.dtstack.dtcenter.common.loader.metadata.core.MetadataBaseCollectSplit;
import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;
import lombok.extern.slf4j.Slf4j;

import java.util.Collections;
import java.util.List;

/**
 * @author luming
 * @date 2022/4/15
 */
@Slf4j
public class HiveCdcMetadataCollectManager extends AbstractMetaDataCollectManager {
    public HiveCdcMetadataCollectManager(ISourceDTO sourceDTO) {
        super(sourceDTO);
    }

    @Override
    protected Class<? extends MetaDataCollect> getMetadataCollectClass() {
        return HiveCdcMetadataCollect.class;
    }

    @Override
    protected List<MetadataBaseCollectSplit> createCollectSplits() {
        //只启动一个监听线程
        return Collections.singletonList(MetadataBaseCollectSplit.builder().build());
    }
}
