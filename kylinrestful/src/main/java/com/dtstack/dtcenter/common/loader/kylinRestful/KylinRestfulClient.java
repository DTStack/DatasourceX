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

package com.dtstack.dtcenter.common.loader.kylinRestful;

import com.dtstack.dtcenter.common.loader.common.nosql.AbsNoSqlClient;
import com.dtstack.dtcenter.common.loader.kylinRestful.request.RestfulClient;
import com.dtstack.dtcenter.common.loader.kylinRestful.request.RestfulClientFactory;
import com.dtstack.dtcenter.loader.dto.ColumnMetaDTO;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;
import com.dtstack.dtcenter.loader.dto.source.KylinRestfulSourceDTO;

import java.util.List;

/**
 * kylin 客户端
 *
 * @author ：qianyi
 * date：Created in 上午10:33 2021/7/14
 * company: www.dtstack.com
 */
public class KylinRestfulClient<T> extends AbsNoSqlClient<T> {


    @Override
    public Boolean testCon(ISourceDTO source) {
        KylinRestfulSourceDTO kylinRestfulSourceDTO = (KylinRestfulSourceDTO) source;
        RestfulClient restfulClient = RestfulClientFactory.getRestfulClient();
        return restfulClient.auth(kylinRestfulSourceDTO);
    }


    @Override
    public List<String> getAllDatabases(ISourceDTO source, SqlQueryDTO queryDTO) {
        KylinRestfulSourceDTO kylinRestfulSourceDTO = (KylinRestfulSourceDTO) source;
        RestfulClient restfulClient = RestfulClientFactory.getRestfulClient();
        return restfulClient.getAllHiveDbList(kylinRestfulSourceDTO);
    }

    @Override
    public List<String> getTableListBySchema(ISourceDTO source, SqlQueryDTO queryDTO) {
        KylinRestfulSourceDTO kylinRestfulSourceDTO = (KylinRestfulSourceDTO) source;
        RestfulClient restfulClient = RestfulClientFactory.getRestfulClient();
        return restfulClient.getAllHiveTableListBySchema(kylinRestfulSourceDTO, queryDTO);
    }

    @Override
    public List<ColumnMetaDTO> getColumnMetaData(ISourceDTO source, SqlQueryDTO queryDTO) {
        KylinRestfulSourceDTO kylinRestfulSourceDTO = (KylinRestfulSourceDTO) source;
        RestfulClient restfulClient = RestfulClientFactory.getRestfulClient();
        return restfulClient.getHiveColumnMetaData(kylinRestfulSourceDTO, queryDTO);
    }

}
