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

package com.dtstack.dtcenter.loader.cache.client.mq;

import com.dtstack.dtcenter.loader.client.IRabbitMq;
import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

/**
 * @author leon
 * @date 2022-06-21 13:03
 **/
@Slf4j
public class RabbitMqProxy implements IRabbitMq {

    private IRabbitMq targetClient;

    public RabbitMqProxy(IRabbitMq targetClient) {
        this.targetClient = targetClient;
    }

    @Override
    public List<List<Object>> getPreview(ISourceDTO source) {
        return targetClient.getPreview(source);
    }

    @Override
    public List<String> getVirtualHosts(ISourceDTO source) {
        return targetClient.getVirtualHosts(source);
    }

    @Override
    public List<String> getQueues(ISourceDTO source) {
        return targetClient.getQueues(source);
    }
}
