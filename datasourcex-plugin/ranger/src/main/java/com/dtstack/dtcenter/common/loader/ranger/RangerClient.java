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

package com.dtstack.dtcenter.common.loader.ranger;

import com.dtstack.dtcenter.common.loader.common.nosql.AbsNoSqlClient;
import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;
import com.dtstack.dtcenter.loader.dto.source.RangerSourceDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.filter.HTTPBasicAuthFilter;
import lombok.extern.slf4j.Slf4j;
import org.apache.ranger.plugin.util.RangerRESTUtils;

/**
 * ranger clinet
 *
 * @author zhiyi
 */
@Slf4j
public class RangerClient extends AbsNoSqlClient {

    /**
     * 获取ranger service url 模板
     */
    private static final String SERVICE_URL_TEMPLATE = "%s/service/public/v2/api/service";

    private static final int RESPONSE_OK_CODE = 200;

    @Override
    public Boolean testCon(ISourceDTO source) {
        RangerSourceDTO sourceDTO = (RangerSourceDTO) source;
        String url = String.format(SERVICE_URL_TEMPLATE, sourceDTO.getUrl());
        String username = sourceDTO.getUsername();
        String password = sourceDTO.getPassword();
        log.info("ranger testCon info url:{}, username:{}, password:{}", sourceDTO.getUrl(), username, password);
        ClientResponse response = null;
        Client client = null;
        try {
            client = Client.create();
            client.addFilter(new HTTPBasicAuthFilter(username, password));
            WebResource webResource = client.resource(url);
            response = webResource.accept(RangerRESTUtils.REST_MIME_TYPE_JSON).get(ClientResponse.class);
            if (response != null && response.getStatus() == RESPONSE_OK_CODE) {
                return true;
            } else if (response != null) {
                throw new DtLoaderException(String.format("connect ranger fail: response code:%s, reason:%s", response.getStatusInfo().getStatusCode(), response.getStatusInfo().getReasonPhrase()));
            }
        } catch (Exception e) {
            throw new DtLoaderException(String.format("connect ranger fail: %s", e.getMessage()), e);
        } finally {
            if (response != null) {
                response.close();
            }
            if (client != null) {
                client.destroy();
            }
        }
        return false;
    }
}
