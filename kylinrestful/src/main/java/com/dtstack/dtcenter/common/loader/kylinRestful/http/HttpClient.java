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

package com.dtstack.dtcenter.common.loader.kylinRestful.http;

import com.dtstack.dtcenter.loader.dto.source.KylinRestfulSourceDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import lombok.extern.slf4j.Slf4j;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

@Slf4j
public class HttpClient {

    public static final Charset DEFAULT_CHARSET = StandardCharsets.UTF_8;

    public static final String POST_REQUEST = "POST";

    public static final String GET_REQUEST = "GET";

    public String authorization;
    /**
     * http 地址、port 管理
     */
    private final HttpAddressManager httpAddressManager;


    HttpClient(KylinRestfulSourceDTO sourceDTO) {
        //用户名密码base64加密
        this.authorization = Base64.getEncoder().encodeToString((sourceDTO.getUsername() + ":" + sourceDTO.getPassword()).getBytes(StandardCharsets.UTF_8));
        this.httpAddressManager = HttpAddressManager.createHttpAddressManager(sourceDTO);
    }

    private String execute(String apiPath, String method, String body) {

        StringBuilder out = new StringBuilder();
        HttpURLConnection connection;
        BufferedReader in;
        try {
            URL url = new URL(apiPath);
            connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod(method);
            connection.setDoOutput(true);
            connection.setRequestProperty("Authorization", "Basic " + authorization);
            connection.setRequestProperty("Content-Type", "application/json");
            if (body != null) {
                byte[] outputInBytes = body.getBytes(StandardCharsets.UTF_8);
                OutputStream os = connection.getOutputStream();
                os.write(outputInBytes);
                os.close();
            }
            InputStream content = connection.getInputStream();
            in = new BufferedReader(new InputStreamReader(content));
            String line;
            while ((line = in.readLine()) != null) {
                out.append(line);
            }
            in.close();
            connection.disconnect();
        } catch (Exception e) {
            throw new DtLoaderException(e.getMessage(), e);
        }
        return out.toString();
    }

    private String getUrl(String apiPath) {
        return this.httpAddressManager.getAddress() + apiPath;
    }


    public String post(String apiPath, String body) {
        String httpFullAPI = getUrl(apiPath);
        return execute(httpFullAPI, POST_REQUEST, body);
    }

    public String get(String apiPath, String body) {
        String httpFullAPI = getUrl(apiPath);
        return execute(httpFullAPI, GET_REQUEST, body);
    }
}
