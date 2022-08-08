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

package com.dtstack.dtcenter.common.loader.rocketmq;

import com.dtstack.dtcenter.common.loader.rocketmq.util.RocketMqUtils;
import com.dtstack.dtcenter.loader.client.IRocketMq;
import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;
import com.dtstack.dtcenter.loader.dto.source.RocketMqSourceDTO;

import java.util.List;

/**
 * @author leon
 * @date 2022-06-21 10:08
 **/
public class RocketMq implements IRocketMq {

    @Override
    public List<String> getTopicList(ISourceDTO source) {
        return getTopicList(source, true);
    }

    @Override
    public List<String> getTopicList(ISourceDTO source, Boolean containSystemTopic) {
        RocketMqSourceDTO rocketMqSourceDTO = (RocketMqSourceDTO) source;
        return RocketMqUtils.getTopicList(rocketMqSourceDTO, containSystemTopic);
    }

    @Override
    public List<List<Object>> getPreview(ISourceDTO source, String prevMode) {
        RocketMqSourceDTO rocketMqSourceDTO = (RocketMqSourceDTO) source;
        return RocketMqUtils.preview(rocketMqSourceDTO, prevMode);
    }
}
