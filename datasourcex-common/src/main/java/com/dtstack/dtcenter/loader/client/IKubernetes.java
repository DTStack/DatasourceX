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

package com.dtstack.dtcenter.loader.client;

import com.dtstack.dtcenter.loader.dto.KubernetesQueryDTO;
import com.dtstack.dtcenter.loader.dto.KubernetesResourceDTO;
import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;
import com.dtstack.rpc.annotation.RpcNodeSign;
import com.dtstack.rpc.annotation.RpcService;
import com.dtstack.rpc.enums.RpcRemoteType;

/**
 * k8s client
 *
 * @author ：wangchuan
 * date：Created in 下午1:50 2022/3/15
 * company: www.dtstack.com
 */
@RpcService(rpcRemoteType = RpcRemoteType.DATASOURCEX_CLIENT)
public interface IKubernetes {

    /**
     * 获取 kubernetes 资源信息
     *
     * @param source        数据源信息
     * @return k8s resource
     */
    KubernetesResourceDTO getResource(@RpcNodeSign("tenantId") ISourceDTO source, KubernetesQueryDTO kubernetesQueryDTO);
}
