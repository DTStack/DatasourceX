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

package com.dtstack.dtcenter.loader.cache.client.kubernetes;

import com.dtstack.dtcenter.loader.cache.client.ClassLoaderCache;
import com.dtstack.dtcenter.loader.callback.ClassLoaderCallBack;
import com.dtstack.dtcenter.loader.callback.ClassLoaderCallBackMethod;
import com.dtstack.dtcenter.loader.client.IKubernetes;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;

import java.util.Iterator;
import java.util.ServiceLoader;

/**
 * <p> k8s 客户端工厂</p>
 *
 * @author ：wangchuan
 * date：Created in 上午10:06 2022/5/7
 * company: www.dtstack.com
 */
public class KubernetesClientFactory {
    public static IKubernetes createPluginClass(String pluginName) throws Exception {
        ClassLoader classLoader = ClassLoaderCache.getClassLoader(pluginName);
        return ClassLoaderCallBackMethod.callbackAndReset((ClassLoaderCallBack<IKubernetes>) () -> {
            ServiceLoader<IKubernetes> kubernetes = ServiceLoader.load(IKubernetes.class);
            Iterator<IKubernetes> iClientIterator = kubernetes.iterator();
            if (!iClientIterator.hasNext()) {
                throw new DtLoaderException("This plugin type is not supported: " + pluginName);
            }
            IKubernetes kubernetesClient = iClientIterator.next();
            return new KubernetesClientProxy(kubernetesClient);
        }, classLoader);
    }
}
