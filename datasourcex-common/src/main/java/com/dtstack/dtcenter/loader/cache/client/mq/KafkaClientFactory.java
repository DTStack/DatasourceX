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

import com.dtstack.dtcenter.loader.client.IKafka;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.dtstack.dtcenter.loader.callback.ClassLoaderCallBack;
import com.dtstack.dtcenter.loader.callback.ClassLoaderCallBackMethod;
import com.dtstack.dtcenter.loader.cache.client.ClassLoaderCache;

import java.util.Iterator;
import java.util.ServiceLoader;

/**
 * @company: www.dtstack.com
 * @Author ：wangchuan
 * @Date ：Created in 下午2:23 2020/6/2
 * @Description：kafka客户端工厂
 */
public class KafkaClientFactory {
    public static IKafka createPluginClass(String pluginName) throws Exception {
        ClassLoader classLoader = ClassLoaderCache.getClassLoader(pluginName);
        return ClassLoaderCallBackMethod.callbackAndReset((ClassLoaderCallBack<IKafka>) () -> {
            ServiceLoader<IKafka> kafkas = ServiceLoader.load(IKafka.class);
            Iterator<IKafka> iClientIterator = kafkas.iterator();
            if (!iClientIterator.hasNext()) {
                throw new DtLoaderException("This plugin type is not supported: " + pluginName);
            }

            IKafka kafka = iClientIterator.next();
            return new KafkaClientProxy(kafka);
        }, classLoader);
    }
}