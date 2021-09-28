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

package com.dtstack.dtcenter.common.loader.opentsdb.tsdb;

import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;
import com.dtstack.dtcenter.loader.dto.source.OpenTSDBSourceDTO;
import lombok.extern.slf4j.Slf4j;

/**
 * Open TSDB 连接工厂
 *
 * @author ：wangchuan
 * date：Created in 上午10:33 2021/6/7
 * company: www.dtstack.com
 */
@Slf4j
public class OpenTSDBConnFactory {

    /**
     * 获取 openTSDB Client，暂不支持连接池
     *
     * @param source 数据源连接信息
     * @return TSDB Client
     */
    public static TSDB getOpenTSDBClient(ISourceDTO source) {
        OpenTSDBSourceDTO openTSDBSourceDTO = (OpenTSDBSourceDTO) source;
        return new TSDBClient(openTSDBSourceDTO);
    }
}
