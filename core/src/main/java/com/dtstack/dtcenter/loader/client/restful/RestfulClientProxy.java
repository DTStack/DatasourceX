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

package com.dtstack.dtcenter.loader.client.restful;

import com.dtstack.dtcenter.loader.ClassLoaderCallBackMethod;
import com.dtstack.dtcenter.loader.client.IRestful;
import com.dtstack.dtcenter.loader.dto.restful.Response;
import com.dtstack.dtcenter.loader.dto.source.RestfulSourceDTO;

import java.io.File;
import java.util.Map;

/**
 * <p> restful 代理类</p>
 *
 * @author ：wangchuan
 * date：Created in 上午10:06 2021/8/11
 * company: www.dtstack.com
 */
public class RestfulClientProxy implements IRestful {

    IRestful targetClient;

    public RestfulClientProxy(IRestful restful) {
        this.targetClient = restful;
    }

    @Override
    public Response get(RestfulSourceDTO sourceDTO, Map<String, String> params, Map<String, String> cookies, Map<String, String> headers) {
        return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.get(sourceDTO, params, cookies, headers),
                targetClient.getClass().getClassLoader());
    }

    @Override
    public Response post(RestfulSourceDTO sourceDTO, String bodyData, Map<String, String> cookies, Map<String, String> headers) {
        return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.post(sourceDTO, bodyData, cookies, headers),
                targetClient.getClass().getClassLoader());
    }

    @Override
    public Response delete(RestfulSourceDTO sourceDTO, String bodyData, Map<String, String> cookies, Map<String, String> headers) {
        return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.delete(sourceDTO, bodyData, cookies, headers),
                targetClient.getClass().getClassLoader());
    }

    @Override
    public Response put(RestfulSourceDTO sourceDTO, String bodyData, Map<String, String> cookies, Map<String, String> headers) {
        return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.put(sourceDTO, bodyData, cookies, headers),
                targetClient.getClass().getClassLoader());
    }

    @Override
    public Response postMultipart(RestfulSourceDTO sourceDTO, Map<String, String> params, Map<String, String> cookies, Map<String, String> headers, Map<String, File> files) {
        return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.postMultipart(sourceDTO, params, cookies, headers, files),
                targetClient.getClass().getClassLoader());
    }
}
