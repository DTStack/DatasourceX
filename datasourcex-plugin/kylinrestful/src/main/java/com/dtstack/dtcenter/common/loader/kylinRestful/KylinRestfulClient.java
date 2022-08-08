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

package com.dtstack.dtcenter.common.loader.kylinRestful;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.dtstack.dtcenter.common.loader.common.nosql.AbsNoSqlClient;
import com.dtstack.dtcenter.common.loader.kylinRestful.request.RestfulClient;
import com.dtstack.dtcenter.common.loader.kylinRestful.request.RestfulClientFactory;
import com.dtstack.dtcenter.loader.client.IJob;
import com.dtstack.dtcenter.loader.dto.ColumnMetaDTO;
import com.dtstack.dtcenter.loader.dto.JobParam;
import com.dtstack.dtcenter.loader.dto.JobResult;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;
import com.dtstack.dtcenter.loader.dto.source.KylinRestfulSourceDTO;
import com.dtstack.dtcenter.loader.enums.JobStatus;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.List;

/**
 * kylin 客户端
 *
 * @author ：qianyi
 * date：Created in 上午10:33 2021/7/14
 * company: www.dtstack.com
 */
@Slf4j
public class KylinRestfulClient extends AbsNoSqlClient implements IJob {

    private static final String KEY_JOB_STATUS = "job_status";
    private static final String KEY_JOB_ID = "uuid";
    private static final String KEY_STEPS = "steps";
    private static final String KEY_STEP_ID = "id";
    private static final String KEY_STEP_STATUS = "step_status";
    private static final String KEY_ERROR_INFO = "error_info";

    @Override
    public Boolean testCon(ISourceDTO source) {
        KylinRestfulSourceDTO kylinRestfulSourceDTO = (KylinRestfulSourceDTO) source;
        RestfulClient restfulClient = RestfulClientFactory.getRestfulClient();
        return restfulClient.auth(kylinRestfulSourceDTO);
    }


    @Override
    public List<String> getAllDatabases(ISourceDTO source, SqlQueryDTO queryDTO) {
        KylinRestfulSourceDTO kylinRestfulSourceDTO = (KylinRestfulSourceDTO) source;
        RestfulClient restfulClient = RestfulClientFactory.getRestfulClient();
        return restfulClient.getAllHiveDbList(kylinRestfulSourceDTO);
    }

    @Override
    public List<String> getTableListBySchema(ISourceDTO source, SqlQueryDTO queryDTO) {
        KylinRestfulSourceDTO kylinRestfulSourceDTO = (KylinRestfulSourceDTO) source;
        RestfulClient restfulClient = RestfulClientFactory.getRestfulClient();
        return restfulClient.getAllHiveTableListBySchema(kylinRestfulSourceDTO, queryDTO);
    }

    @Override
    public List<ColumnMetaDTO> getColumnMetaData(ISourceDTO source, SqlQueryDTO queryDTO) {
        KylinRestfulSourceDTO kylinRestfulSourceDTO = (KylinRestfulSourceDTO) source;
        RestfulClient restfulClient = RestfulClientFactory.getRestfulClient();
        return restfulClient.getHiveColumnMetaData(kylinRestfulSourceDTO, queryDTO);
    }

    @Override
    public JobResult submitJob(ISourceDTO source, JobParam jobParam) {
        KylinRestfulSourceDTO sourceDTO = (KylinRestfulSourceDTO) source;
        return BooleanUtils.isTrue(jobParam.getRetry()) ?
                resumeJob(sourceDTO, jobParam) : createNewJobInstance(sourceDTO, jobParam);
    }

    @Override
    public JobResult cancelJob(ISourceDTO source, JobParam jobParam) {
        if (StringUtils.isBlank(jobParam.getJobId())) {
            return JobResult.createErrorResult("must need jobId");
        }
        try {
            KylinRestfulSourceDTO sourceDTO = (KylinRestfulSourceDTO) source;
            RestfulClientFactory.getRestfulClient().discardJob(sourceDTO, jobParam);
        } catch (Exception e) {
            return JobResult.createErrorResult("cancel job error " + e.getMessage());
        }
        return JobResult.createSuccessResult(jobParam.getJobId(), jobParam.getJobId());
    }

    private JobResult resumeJob(KylinRestfulSourceDTO source, JobParam jobParam) {
        JSONObject lastJob = getLastJob(source, jobParam);
        if (lastJob == null) {
            return createNewJobInstance(source, jobParam);
        }

        String jobId = lastJob.getString(KEY_JOB_ID);
        try {
            if (RestfulClientFactory.getRestfulClient().resumeJob(source, jobId, jobParam)) {
                return JobResult.createSuccessResult(jobId, jobId);
            }
        } catch (Exception e) {
            return JobResult.createErrorResult("resume job error : " + e.getMessage());
        }

        return null;
    }

    private JobResult createNewJobInstance(KylinRestfulSourceDTO source, JobParam jobParam) {
        checkParamOfNewJob(jobParam);
        discardErrorJob(source, jobParam);
        try {
            String jobId = RestfulClientFactory.getRestfulClient().buildCube(source, jobParam);
            return JobResult.createSuccessResult(jobId, jobId);
        } catch (Exception e) {
            return JobResult.createErrorResult("create new job error " + e.getMessage());
        }
    }

    private void checkParamOfNewJob(JobParam jobParam) {
        if (StringUtils.isBlank(jobParam.getCubeName())) {
            throw new DtLoaderException("when create new job, cubeName param must need");
        }
        if (jobParam.getStartTime() == null || jobParam.getEndTime() == null) {
            throw new DtLoaderException("when create new job, time param must need");
        }
    }

    private void discardErrorJob(KylinRestfulSourceDTO source, JobParam jobParam) {
        JSONObject lastJob = getLastJob(source, jobParam);
        if (lastJob == null) {
            return;
        }

        if (JobStatus.ERROR.name().equals(lastJob.getString(KEY_JOB_STATUS))) {
            String jobId = lastJob.getString(KEY_JOB_ID);
            jobParam.setJobId(jobId);
            RestfulClientFactory.getRestfulClient().discardJob(source, jobParam);
        }
    }

    private JSONObject getLastJob(KylinRestfulSourceDTO source, JobParam jobParam) {
        List<JSONObject> jobList = RestfulClientFactory.getRestfulClient().getJobList(source, jobParam, 1);
        if (CollectionUtils.isEmpty(jobList)) {
            return null;
        }
        return jobList.get(0);
    }

    @Override
    public String getJobStatus(ISourceDTO source, JobParam jobParam) {
        if (StringUtils.isBlank(jobParam.getJobId())) {
            throw new DtLoaderException("must need jobId");
        }
        KylinRestfulSourceDTO sourceDTO = (KylinRestfulSourceDTO) source;
        return RestfulClientFactory.getRestfulClient().getJobStatus(sourceDTO, jobParam.getJobId(), jobParam);
    }

    @Override
    public String getJobLog(ISourceDTO source, JobParam jobParam) {
        if (StringUtils.isBlank(jobParam.getJobId())) {
            throw new DtLoaderException("must need jobId");
        }
        KylinRestfulSourceDTO sourceDTO = (KylinRestfulSourceDTO) source;
        String res = RestfulClientFactory.getRestfulClient().getJobInfo(sourceDTO, jobParam.getJobId(), jobParam);
        JSONObject resJson = JSONObject.parseObject(res);
        if (resJson == null) {
            return "";
        }
        JSONArray steps = resJson.getJSONArray(KEY_STEPS);
        if (CollectionUtils.isEmpty(steps)) {
            return "";
        }
        for (int i = 0; i < steps.size(); i++) {
            JSONObject step = steps.getJSONObject(i);
            String stepStatus = step.getString(KEY_STEP_STATUS);
            String stepId = step.getString(KEY_STEP_ID);
            if (JobStatus.ERROR.name().equals(stepStatus)) {
                step.put(KEY_ERROR_INFO, getErrorLog(sourceDTO, jobParam.getJobId(), stepId, jobParam));
                break;
            }
        }
        return resJson.toJSONString();
    }

    private String getErrorLog(KylinRestfulSourceDTO source, String jobId, String stepId, JobParam jobParam) {
        return RestfulClientFactory.getRestfulClient().getErrorLog(source, jobId, jobParam, stepId);
    }

    @Override
    public Boolean judgeSlots(ISourceDTO source, JobParam jobParam) {
        KylinRestfulSourceDTO sourceDTO = (KylinRestfulSourceDTO) source;
        JSONObject lastJob = getLastJob(sourceDTO, jobParam);
        if (lastJob == null) {
            return true;
        }

        String status = lastJob.getString(KEY_JOB_STATUS);
        if (JobStatus.PENDING.name().equals(status) || JobStatus.RUNNING.name().equals(status)) {
            log.info("The last job of cube {} is in status {}, waiting for it to finish", jobParam.getCubeName(), status);
            return false;
        }
        if (JobStatus.STOPPED.name().equals(status)) {
            log.warn("The last job of cube {} is in status {},please resume or discard it first", jobParam.getCubeName(), status);
            return false;
        }

        return true;
    }
}
