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

package com.dtstack.dtcenter.common.loader.es.metadata;

import com.dtstack.dtcenter.common.loader.metadata.Manager.AbstractMetaDataCollectManager;
import com.dtstack.dtcenter.common.loader.metadata.collect.MetaDataCollect;
import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;

/**
 * @author luming
 * @date 2022/4/12
 */
public class EsMetadataCollectManager extends AbstractMetaDataCollectManager {
    public EsMetadataCollectManager(ISourceDTO sourceDTO) {
        super(sourceDTO);
    }

    @Override
    protected Class<? extends MetaDataCollect> getMetadataCollectClass() {
        return EsMetadataCollect.class;
    }
}
