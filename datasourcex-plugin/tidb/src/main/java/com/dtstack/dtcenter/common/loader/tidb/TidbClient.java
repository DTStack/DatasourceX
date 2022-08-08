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

package com.dtstack.dtcenter.common.loader.tidb;

import com.dtstack.dtcenter.common.loader.mysql5.MysqlClient;
import com.dtstack.dtcenter.common.loader.rdbms.ConnFactory;
import com.dtstack.dtcenter.common.loader.tidb.metadata.TidbMetaDataCollectManager;
import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;
import com.dtstack.dtcenter.loader.metadata.MetaDataCollectManager;
import com.dtstack.dtcenter.loader.source.DataSourceType;

/**
 * @author luming
 * @date 2022/2/23
 */
public class TidbClient extends MysqlClient{
    @Override
    protected ConnFactory getConnFactory() {
        return new TidbConnFactory();
    }

    @Override
    protected DataSourceType getSourceType() {
        return DataSourceType.TiDB;
    }

    @Override
    public MetaDataCollectManager getMetadataCollectManager(ISourceDTO sourceDTO) {
        return new TidbMetaDataCollectManager(sourceDTO);
    }
}
