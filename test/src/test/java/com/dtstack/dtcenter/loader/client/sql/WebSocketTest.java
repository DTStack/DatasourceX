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

package com.dtstack.dtcenter.loader.client.sql;

import com.dtstack.dtcenter.loader.client.BaseTest;
import com.dtstack.dtcenter.loader.client.ClientCache;
import com.dtstack.dtcenter.loader.client.IClient;
import com.dtstack.dtcenter.loader.dto.source.WebSocketSourceDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import org.junit.Ignore;
import org.junit.Test;

import java.util.HashMap;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 21:24 2020/11/25
 * @Description：WebSocket 测试类
 */
@Ignore
public class WebSocketTest extends BaseTest {

    /**
     * 没有对应的数据源信息
     */
    @Test
    public void testCon() throws Exception {
        HashMap<String, String> map = new HashMap<>();
        map.put("userName", "nanqi");
        WebSocketSourceDTO sourceDTO = WebSocketSourceDTO.builder().url("ws://127.0.0.1:8080/nanqi").authParams(map).build();
        IClient client = ClientCache.getClient(DataSourceType.WEB_SOCKET.getVal());
        Boolean isConnected = client.testCon(sourceDTO);
        if (Boolean.FALSE.equals(isConnected)) {
            throw new DtLoaderException("connection exception");
        }
    }
}
