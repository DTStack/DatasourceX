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

package com.dtstack.dtcenter.common.loader.rabbitmq;

import com.dtstack.dtcenter.common.loader.rabbitmq.util.RabbitMqUtils;
import com.dtstack.dtcenter.loader.client.IRabbitMq;
import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;
import com.dtstack.dtcenter.loader.dto.source.RabbitMqSourceDTO;

import java.util.List;

/**
 * @author leon
 * @date 2022-07-19 17:19
 **/
public class RabbitMq implements IRabbitMq {

    @Override
    public List<List<Object>> getPreview(ISourceDTO source) {
        RabbitMqSourceDTO sourceDTO = (RabbitMqSourceDTO) source;
        return RabbitMqUtils.preview(sourceDTO);
    }

    @Override
    public List<String> getVirtualHosts(ISourceDTO source) {
        RabbitMqSourceDTO sourceDTO = (RabbitMqSourceDTO) source;
        return RabbitMqUtils.getVirtualHosts(sourceDTO);
    }

    @Override
    public List<String> getQueues(ISourceDTO source) {
        RabbitMqSourceDTO sourceDTO = (RabbitMqSourceDTO) source;
        return RabbitMqUtils.getQueues(sourceDTO);
    }
}
