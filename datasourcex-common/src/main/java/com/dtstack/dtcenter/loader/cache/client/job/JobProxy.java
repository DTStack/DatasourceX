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

package com.dtstack.dtcenter.loader.cache.client.job;

import com.dtstack.dtcenter.loader.callback.ClassLoaderCallBackMethod;
import com.dtstack.dtcenter.loader.client.IJob;
import com.dtstack.dtcenter.loader.dto.JobParam;
import com.dtstack.dtcenter.loader.dto.JobResult;
import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;

/**
 * @author luming
 * @date 2022/2/25
 */
public class JobProxy implements IJob {

    private final IJob targetClient;

    public JobProxy(IJob iJob) {
        this.targetClient = iJob;
    }

    @Override
    public JobResult submitJob(ISourceDTO source, JobParam jobParam) {
        return ClassLoaderCallBackMethod.callbackAndReset(
                () -> targetClient.submitJob(source, jobParam), targetClient.getClass().getClassLoader());
    }

    @Override
    public JobResult cancelJob(ISourceDTO source, JobParam jobParam) {
        return ClassLoaderCallBackMethod.callbackAndReset(
                () -> targetClient.cancelJob(source, jobParam), targetClient.getClass().getClassLoader());
    }

    @Override
    public String getJobStatus(ISourceDTO source, JobParam jobParam) {
        return ClassLoaderCallBackMethod.callbackAndReset(
                () -> targetClient.getJobStatus(source, jobParam), targetClient.getClass().getClassLoader());
    }

    @Override
    public String getJobLog(ISourceDTO source, JobParam jobParam) {
        return ClassLoaderCallBackMethod.callbackAndReset(
                () -> targetClient.getJobLog(source, jobParam), targetClient.getClass().getClassLoader());
    }

    @Override
    public Boolean judgeSlots(ISourceDTO source, JobParam jobParam) {
        return ClassLoaderCallBackMethod.callbackAndReset(
                () -> targetClient.judgeSlots(source, jobParam), targetClient.getClass().getClassLoader());
    }
}
