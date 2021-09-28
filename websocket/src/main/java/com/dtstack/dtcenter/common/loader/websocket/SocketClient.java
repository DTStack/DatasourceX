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

package com.dtstack.dtcenter.common.loader.websocket;

import com.dtstack.dtcenter.common.loader.common.exception.IErrorPattern;
import com.dtstack.dtcenter.common.loader.common.nosql.AbsNoSqlClient;
import com.dtstack.dtcenter.common.loader.common.service.ErrorAdapterImpl;
import com.dtstack.dtcenter.common.loader.common.service.IErrorAdapter;
import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;
import com.dtstack.dtcenter.loader.dto.source.WebSocketSourceDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.java_websocket.WebSocket;
import org.java_websocket.client.WebSocketClient;
import org.java_websocket.handshake.ServerHandshake;

import java.net.URI;
import java.util.stream.Collectors;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 13:01 2020/11/25
 * @Description：Socket 客户端
 */
@Slf4j
public class SocketClient<T> extends AbsNoSqlClient<T> {
    /**
     * socket 地址
     */
    private static final String SOCKET_URL = "%s?%s";

    private static final IErrorPattern ERROR_PATTERN = new WebsocketErrorPattern();

    // 异常适配器
    private static final IErrorAdapter ERROR_ADAPTER = new ErrorAdapterImpl();

    @Override
    public Boolean testCon(ISourceDTO source) {
        WebSocketSourceDTO socketSourceDTO = (WebSocketSourceDTO) source;
        String authParamStr = null;
        if (MapUtils.isNotEmpty(socketSourceDTO.getAuthParams())) {
            authParamStr = socketSourceDTO.getAuthParams().entrySet().stream()
                    .map(entry -> entry.getKey() + "=" + entry.getValue()).collect(Collectors.joining("&"));
        }
        WebSocketClient myClient = null;
        try {
            String url = StringUtils.isNotBlank(authParamStr) ? String.format(SOCKET_URL, socketSourceDTO.getUrl(), authParamStr) : socketSourceDTO.getUrl();
            myClient = new WebSocketClient(new URI(url)) {
                @Override
                public void onOpen(ServerHandshake handshakedata) {
                    log.info("socket connected successfully");
                }

                @Override
                public void onMessage(String message) {
                }

                @Override
                public void onClose(int code, String reason, boolean remote) {
                    log.info("socket close was succeeded");
                }

                @Override
                public void onError(Exception ex) {
                    throw new DtLoaderException(ex.getMessage(), ex);
                }
            };
            myClient.connect();
            // 判断是否连接成功，未成功后面发送消息时会报错
            int maxRetry = 0;
            while (!WebSocket.READYSTATE.OPEN.equals(myClient.getReadyState()) && maxRetry < 5) {
                maxRetry++;
                Thread.sleep(1000);
            }
            return WebSocket.READYSTATE.OPEN.equals(myClient.getReadyState());
        } catch (Exception e) {
            throw new DtLoaderException(ERROR_ADAPTER.connAdapter(e.getMessage(), ERROR_PATTERN), e);
        } finally {
            if (myClient != null) {
                myClient.close();
            }
        }
    }

}
