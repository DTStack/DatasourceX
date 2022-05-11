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

package com.dtstack.dtcenter.common.loader.restful;

import com.dtstack.dtcenter.common.loader.restful.http.HttpClient;
import com.dtstack.dtcenter.common.loader.restful.http.HttpClientFactory;
import com.dtstack.dtcenter.loader.client.IRestful;
import com.dtstack.dtcenter.loader.dto.restful.Response;
import com.dtstack.dtcenter.loader.dto.source.RestfulSourceDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;

import java.io.File;
import java.io.IOException;
import java.util.Map;

/**
 * restful 特有客户端
 *
 * @author ：wangchuan
 * date：Created in 上午10:38 2021/8/11
 * company: www.dtstack.com
 */
public class RestfulSpecialClient implements IRestful {

    @Override
    public Response get(RestfulSourceDTO sourceDTO, Map<String, String> params, Map<String, String> cookies, Map<String, String> headers) {
        try (HttpClient httpClient = HttpClientFactory.createHttpClientAndStart(sourceDTO)) {
            return httpClient.get(params, cookies, headers);
        } catch (IOException e) {
            throw new DtLoaderException(e.getMessage(), e);
        }
    }

    @Override
    public Response post(RestfulSourceDTO sourceDTO, String bodyData, Map<String, String> cookies, Map<String, String> headers) {
        try (HttpClient httpClient = HttpClientFactory.createHttpClientAndStart(sourceDTO)) {
            return httpClient.post(bodyData, cookies, headers);
        } catch (IOException e) {
            throw new DtLoaderException(e.getMessage(), e);
        }
    }

    @Override
    public Response delete(RestfulSourceDTO sourceDTO, String bodyData, Map<String, String> cookies, Map<String, String> headers) {
        try (HttpClient httpClient = HttpClientFactory.createHttpClientAndStart(sourceDTO)) {
            return httpClient.delete(bodyData, cookies, headers);
        } catch (IOException e) {
            throw new DtLoaderException(e.getMessage(), e);
        }
    }

    @Override
    public Response put(RestfulSourceDTO sourceDTO, String bodyData, Map<String, String> cookies, Map<String, String> headers) {
        try (HttpClient httpClient = HttpClientFactory.createHttpClientAndStart(sourceDTO)) {
            return httpClient.put(bodyData, cookies, headers);
        } catch (IOException e) {
            throw new DtLoaderException(e.getMessage(), e);
        }
    }

    @Override
    public Response postMultipart(RestfulSourceDTO sourceDTO, Map<String, String> params, Map<String, String> cookies, Map<String, String> headers, Map<String, File> files) {
        try (HttpClient httpClient = HttpClientFactory.createHttpClientAndStart(sourceDTO)) {
            return httpClient.postMultipart(params, cookies, headers, files);
        } catch (IOException e) {
            throw new DtLoaderException(e.getMessage(), e);
        }
    }
}
