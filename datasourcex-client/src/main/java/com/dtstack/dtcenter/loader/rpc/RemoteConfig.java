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

package com.dtstack.dtcenter.loader.rpc;

import com.dtstack.rpc.annotation.RpcEnable;
import com.dtstack.rpc.enums.RpcRemoteType;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;

/**
 * remote client 配置类, 只有开启远程调用时再加载该类
 *
 * @author ：wangchuan
 * date：Created in 下午5:22 2021/12/22
 * company: www.dtstack.com
 */
@RpcEnable(
        basePackage = "com.dtstack.dtcenter.loader.client",
        remoteType = RpcRemoteType.DATASOURCEX_CLIENT)
@Configuration
@ConditionalOnProperty(prefix = "datasourcex.rpc.scan",name = "open",havingValue = "true",matchIfMissing = true)
public class RemoteConfig {
}
