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

package com.dtstack.dtcenter.common.loader.kubernetes.client;

import com.alibaba.fastjson.JSON;
import com.dtstack.dtcenter.common.loader.common.utils.ListUtil;
import com.dtstack.dtcenter.loader.client.IKubernetes;
import com.dtstack.dtcenter.loader.dto.KubernetesQueryDTO;
import com.dtstack.dtcenter.loader.dto.KubernetesResourceDTO;
import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;
import com.dtstack.dtcenter.loader.dto.source.KubernetesSourceDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import io.fabric8.kubernetes.api.model.NodeList;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodList;
import io.fabric8.kubernetes.api.model.ResourceQuotaList;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * kubernetes specific client
 *
 * @author ：wangchuan
 * date：Created in 下午2:08 2022/3/15
 * company: www.dtstack.com
 */
@Slf4j
public class KubernetesSpecificClient implements IKubernetes {

    /**
     * 默认 namespace
     */
    private static final String DEFAULT_NAMESPACE = "default";

    @Override
    public KubernetesResourceDTO getResource(ISourceDTO source, KubernetesQueryDTO kubernetesQueryDTO) {
        String namespace = Objects.isNull(kubernetesQueryDTO) ? DEFAULT_NAMESPACE :
                (StringUtils.isBlank(kubernetesQueryDTO.getNamespace()) ? DEFAULT_NAMESPACE :kubernetesQueryDTO.getNamespace());
        List<String> podStatus = Objects.isNull(kubernetesQueryDTO) ? null : kubernetesQueryDTO.getPodStatus();

        KubernetesSourceDTO kubernetesSourceDTO = (KubernetesSourceDTO) source;
        io.fabric8.kubernetes.client.KubernetesClient client = null;
        try {
            String kubernetesConf = kubernetesSourceDTO.getKubernetesConf();
            io.fabric8.kubernetes.client.Config kubernetes = io.fabric8.kubernetes.client.Config.fromKubeconfig(kubernetesConf);
            client = new DefaultKubernetesClient(kubernetes);
            ResourceQuotaList quotaList = client.resourceQuotas()
                    .inNamespace(namespace)
                    .list();

            PodList podList = client.pods()
                    .inNamespace(namespace)
                    .list();

            if (CollectionUtils.isNotEmpty(podStatus)) {
                List<Pod> itemsBefore = podList.getItems();
                List<Pod> itemsAfter = itemsBefore.stream()
                        .filter(item -> ListUtil.containsIgnoreCase(podStatus, item.getStatus().getPhase()))
                        .collect(Collectors.toList());
                podList.setItems(itemsAfter);
            }

            NodeList nodeList = client.
                    nodes().
                    list();

            return KubernetesResourceDTO.builder()
                    .nodeList(JSON.toJSONString(nodeList))
                    .podList(JSON.toJSONString(podList))
                    .resourceQuotaList(JSON.toJSONString(quotaList)).build();
        } catch (Exception e) {
            throw new DtLoaderException(String.format("get kubernetes resource error: %s", e.getMessage()), e);
        } finally {
            if (Objects.nonNull(client)) {
                client.close();
            }
        }
    }
}