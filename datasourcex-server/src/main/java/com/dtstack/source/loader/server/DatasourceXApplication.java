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

package com.dtstack.source.loader.server;

import com.dtstack.dtcenter.loader.utils.SystemPropertyUtil;
import com.dtstack.rpc.annotation.RpcEnable;
import com.dtstack.rpc.enums.RpcRemoteType;
import com.dtstack.source.loader.server.proxy.ServerImplProxy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.Banner;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;

/**
 * DatasourceX springboot 启动类, 以非 web 项目启动
 *
 * @author ：wangchuan
 * date：Created in 下午3:38 2021/12/17
 * company: www.dtstack.com
 */
@SpringBootApplication
@RpcEnable(
        remoteType = RpcRemoteType.DATASOURCEX_SERVER,
        basePackage = "com.dtstack.dtcenter.loader.client",
        proxyClass = ServerImplProxy.class)
@Slf4j
public class DatasourceXApplication implements CommandLineRunner {

    public static void main(String[] args) {
        try {
            SystemPropertyUtil.setSystemUserDir();
            // 启动应用
            new SpringApplicationBuilder(DatasourceXApplication.class)
                    .web(WebApplicationType.NONE)
                    .bannerMode(Banner.Mode.OFF)
                    .run(args);
        } catch (Throwable th) {
            log.error("start DatasourceX error:", th);
            System.exit(-1);
        }
    }

    @Override
    public void run(String... args) throws Exception {
        Thread.currentThread().join();
    }
}
