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

import com.dtstack.dtcenter.loader.dto.JobParam;
import com.dtstack.dtcenter.loader.dto.JobResult;
import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;
import com.dtstack.rpc.annotation.RpcNodeSign;
import com.dtstack.rpc.annotation.RpcService;
import com.dtstack.rpc.enums.RpcRemoteType;

/**
 * 任务相关接口类
 *
 * @author luming
 * @date 2022/2/25
 */
@RpcService(rpcRemoteType = RpcRemoteType.DATASOURCEX_CLIENT)
public interface IJob {
    /**
     * 提交job
     *
     * @param source 数据源相关信息
     * @param jobParam 任务相关参数
     * @return job创建后生成的唯一id
     */
    JobResult submitJob(@RpcNodeSign("tenantId") ISourceDTO source, JobParam jobParam);
    /**
     * 取消job
     *
     * @param source 数据源相关信息
     * @param jobParam 任务相关参数
     * @return 成功取消返回jobId
     */
    JobResult cancelJob(@RpcNodeSign("tenantId") ISourceDTO source, JobParam jobParam);
    /**
     * 获取job当前状态
     *
     * @param source 数据源相关信息
     * @param jobParam 任务相关参数
     * @return 状态信息
     */
    String getJobStatus(@RpcNodeSign("tenantId") ISourceDTO source, JobParam jobParam);
    /**
     * 获取任务日志
     *
     * @param source 数据源相关信息
     * @param jobParam 任务相关参数
     * @return 日志
     */
    String getJobLog(@RpcNodeSign("tenantId") ISourceDTO source, JobParam jobParam);
    /**
     *
     *
     * @param source 数据源相关信息
     * @param jobParam 任务相关参数
     * @return
     */
    Boolean judgeSlots(@RpcNodeSign("tenantId") ISourceDTO source, JobParam jobParam);
}
