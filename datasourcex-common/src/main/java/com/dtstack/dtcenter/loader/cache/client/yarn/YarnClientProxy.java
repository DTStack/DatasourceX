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

package com.dtstack.dtcenter.loader.cache.client.yarn;

import com.dtstack.dtcenter.loader.callback.ClassLoaderCallBackMethod;
import com.dtstack.dtcenter.loader.client.IYarn;
import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;
import com.dtstack.dtcenter.loader.dto.yarn.YarnApplicationInfoDTO;
import com.dtstack.dtcenter.loader.dto.yarn.YarnApplicationStatus;
import com.dtstack.dtcenter.loader.dto.yarn.YarnResourceDTO;
import com.dtstack.dtcenter.loader.dto.yarn.YarnResourceDescriptionDTO;

import java.util.List;

/**
 * <p> yarn 代理类</p>
 *
 * @author ：wangchuan
 * date：Created in 上午10:06 2022/3/17
 * company: www.dtstack.com
 */
public class YarnClientProxy implements IYarn {

    IYarn targetClient;

    public YarnClientProxy(IYarn yarn) {
        this.targetClient = yarn;
    }

    @Override
    public List<YarnApplicationInfoDTO> listApplication(ISourceDTO source, YarnApplicationStatus status, String taskName, String applicationId) {
        return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.listApplication(source, status, taskName, applicationId),
                targetClient.getClass().getClassLoader());
    }

    @Override
    public YarnResourceDTO getYarnResource(ISourceDTO source) {
        return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.getYarnResource(source),
                targetClient.getClass().getClassLoader());
    }

    @Override
    public YarnResourceDescriptionDTO getYarnResourceDescription(ISourceDTO source) {
        return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.getYarnResourceDescription(source),
                targetClient.getClass().getClassLoader());
    }
}
