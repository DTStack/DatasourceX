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

package com.dtstack.dtcenter.loader.client;

import com.dtstack.dtcenter.loader.dto.RedisQueryDTO;
import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;
import com.dtstack.rpc.annotation.RpcNodeSign;
import com.dtstack.rpc.annotation.RpcService;
import com.dtstack.rpc.enums.RpcRemoteType;

import java.util.List;
import java.util.Map;

/**
 * <p>提供 redis 相关操作方法</p>
 *
 * @author ：qianyi
 * date：Created in 上午10:06 2021/8/16
 * company: www.dtstack.com
 */
@RpcService(rpcRemoteType = RpcRemoteType.DATASOURCEX_CLIENT)
public interface IRedis {
    /**
     * redis 自定义查询
     *
     * @param source   数据源信息
     * @param queryDTO sql 执行条件
     * @return sql 执行结果
     */
    Map<String, Object> executeQuery(@RpcNodeSign("tenantId") ISourceDTO source, RedisQueryDTO queryDTO);

    /**
     * 预览 redis key值
     *
     * @param source   数据源信息
     * @param queryDTO 查询条件
     * @return key值
     */
    List<String> preViewKey(@RpcNodeSign("tenantId") ISourceDTO source, RedisQueryDTO queryDTO);
}
