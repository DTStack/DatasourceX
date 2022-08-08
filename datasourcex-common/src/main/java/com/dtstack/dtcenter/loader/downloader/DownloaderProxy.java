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

package com.dtstack.dtcenter.loader.downloader;

import com.dtstack.rpc.download.IDownloader;
import com.dtstack.dtcenter.loader.callback.ClassLoaderCallBackMethod;

import java.util.List;

/**
 * IDownloader代理类
 * 若不加此代理类，由于获取到IDownloader后，当前线程的ContextClassLoader会被设置成旧的ClassLoader
 * 这样在执行IDownloader中方法的时候如果方法中有对线程ContextClassLoader进行获取操作则会导致ClassLoader不一致
 *
 * @author ：wangchuan
 * date：Created in 下午1:15 2020/8/12
 * company: www.dtstack.com
 */
public class DownloaderProxy implements IDownloader {

    private IDownloader targetDownloader;

    public DownloaderProxy(IDownloader targetDownloader) {
        this.targetDownloader = targetDownloader;
    }

    @Override
    public boolean configure() throws Exception {
        return ClassLoaderCallBackMethod.callbackAndReset(() -> targetDownloader.configure(),
                targetDownloader.getClass().getClassLoader());
    }

    @Override
    public List<String> getMetaInfo() {
        return ClassLoaderCallBackMethod.callbackAndReset(() -> targetDownloader.getMetaInfo(),
                targetDownloader.getClass().getClassLoader());
    }

    @Override
    public Object readNext() {
        return ClassLoaderCallBackMethod.callbackAndReset(() -> targetDownloader.readNext(),
                targetDownloader.getClass().getClassLoader());
    }

    @Override
    public boolean reachedEnd() {
        return ClassLoaderCallBackMethod.callbackAndReset(() -> targetDownloader.reachedEnd(),
                targetDownloader.getClass().getClassLoader());
    }

    @Override
    public boolean close() throws Exception {
        return ClassLoaderCallBackMethod.callbackAndReset(() -> targetDownloader.close(),
                targetDownloader.getClass().getClassLoader());
    }

    @Override
    public String getUniqueKey() {
        return ClassLoaderCallBackMethod.callbackAndReset(() -> targetDownloader.getUniqueKey(),
                targetDownloader.getClass().getClassLoader());
    }

    @Override
    public void setUniqueKey(String uniqueKey) {
        targetDownloader.setUniqueKey(uniqueKey);
    }
}
