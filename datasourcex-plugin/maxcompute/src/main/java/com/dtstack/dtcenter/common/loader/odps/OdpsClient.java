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

package com.dtstack.dtcenter.common.loader.odps;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.aliyun.odps.Column;
import com.aliyun.odps.Instance;
import com.aliyun.odps.Job;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.Table;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.Tables;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.data.DefaultRecordReader;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.ResultSet;
import com.aliyun.odps.task.SQLTask;
import com.dtstack.dtcenter.common.loader.common.exception.IErrorPattern;
import com.dtstack.dtcenter.common.loader.common.nosql.AbsNoSqlClient;
import com.dtstack.dtcenter.common.loader.common.service.ErrorAdapterImpl;
import com.dtstack.dtcenter.common.loader.common.service.IErrorAdapter;
import com.dtstack.dtcenter.common.loader.common.utils.SearchUtil;
import com.dtstack.dtcenter.common.loader.odps.common.OdpsFields;
import com.dtstack.dtcenter.common.loader.odps.pool.OdpsManager;
import com.dtstack.dtcenter.common.loader.odps.pool.OdpsPool;
import com.dtstack.dtcenter.loader.client.IJob;
import com.dtstack.dtcenter.loader.dto.ColumnMetaDTO;
import com.dtstack.dtcenter.loader.dto.JobParam;
import com.dtstack.dtcenter.loader.dto.JobResult;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;
import com.dtstack.dtcenter.loader.dto.source.OdpsSourceDTO;
import com.dtstack.dtcenter.loader.enums.ConnectionClearStatus;
import com.dtstack.dtcenter.loader.enums.JobStatus;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 17:47 2020/1/7
 * @Description：ODPS 客户端
 */
@Slf4j
public class OdpsClient extends AbsNoSqlClient implements IJob {

    public static final ThreadLocal<Boolean> IS_OPEN_POOL = new ThreadLocal<>();

    private static OdpsManager odpsManager = OdpsManager.getInstance();

    private static final IErrorPattern ERROR_PATTERN = new OdpsErrorPattern();

    // 异常适配器
    private static final IErrorAdapter ERROR_ADAPTER = new ErrorAdapterImpl();

    private static final String SPLIT = ";";

    @Override
    public Boolean testCon(ISourceDTO iSource) {
        OdpsSourceDTO odpsSourceDTO = (OdpsSourceDTO) iSource;
        Odps odps = null;
        try {
            odps = initOdps(odpsSourceDTO);
            Tables tables = odps.tables();
            tables.iterator().hasNext();
            return true;
        } catch (Exception ex) {
            throw new DtLoaderException(ERROR_ADAPTER.connAdapter(ex.getMessage(), ERROR_PATTERN), ex);
        } finally {
            closeResource(odps, odpsSourceDTO);
        }
    }

    public static Odps initOdps(OdpsSourceDTO odpsSourceDTO) {
        JSONObject odpsConfig = JSON.parseObject(odpsSourceDTO.getConfig());

        boolean check = odpsSourceDTO.getPoolConfig() != null;
        IS_OPEN_POOL.set(check);
        //不开启连接池
        if (!check) {
            return initOdps(odpsConfig.getString(OdpsFields.KEY_ODPS_SERVER), odpsConfig.getString(OdpsFields.KEY_ACCESS_ID),
                    odpsConfig.getString(OdpsFields.KEY_ACCESS_KEY), odpsConfig.getString(OdpsFields.KEY_PROJECT),
                    odpsConfig.getString(OdpsFields.PACKAGE_AUTHORIZED_PROJECT), odpsConfig.getString(OdpsFields.KEY_ACCOUNT_TYPE));
        }
        //开启连接池
        OdpsPool odpsPool = odpsManager.getConnection(odpsSourceDTO);
        Odps odps = odpsPool.getResource();
        if (Objects.isNull(odps)) {
            throw new DtLoaderException("No database connection available");
        }
        return odps;
    }

    public static Odps initOdps(String odpsServer, String accessId, String accessKey, String project,
                                String packageAuthorizedProject, String accountType) {
        if (StringUtils.isBlank(odpsServer)) {
            odpsServer = OdpsFields.DEFAULT_ODPS_SERVER;
        }

        if (StringUtils.isBlank(accessId)) {
            throw new IllegalArgumentException("accessId is required");
        }

        if (StringUtils.isBlank(accessKey)) {
            throw new IllegalArgumentException("accessKey is required");
        }

        if (StringUtils.isBlank(project)) {
            throw new IllegalArgumentException("project is required");
        }

        String defaultProject;
        if (StringUtils.isBlank(packageAuthorizedProject)) {
            defaultProject = project;
        } else {
            defaultProject = packageAuthorizedProject;
        }

        if (StringUtils.isBlank(accountType)) {
            accountType = OdpsFields.DEFAULT_ACCOUNT_TYPE;
        }

        Account account = null;
        if (accountType.equalsIgnoreCase(OdpsFields.DEFAULT_ACCOUNT_TYPE)) {
            account = new AliyunAccount(accessId, accessKey);
        } else {
            throw new IllegalArgumentException("Unsupported account type: " + accountType);
        }

        Odps odps = new Odps(account);
        odps.getRestClient().setConnectTimeout(3);
        odps.getRestClient().setReadTimeout(10);
        odps.getRestClient().setRetryTimes(2);
        odps.setDefaultProject(defaultProject);
        odps.setEndpoint(odpsServer);
        return odps;
    }

    @Override
    public List<String> getTableList(ISourceDTO iSource, SqlQueryDTO queryDTO) {
        OdpsSourceDTO odpsSourceDTO = (OdpsSourceDTO) iSource;
        beforeQuery(odpsSourceDTO, queryDTO, false);
        List<String> tableList = new ArrayList<>();
        Odps odps = null;
        try {
            odps = initOdps(odpsSourceDTO);
            odps.tables().forEach((Table table) -> tableList.add(table.getName()));
        } catch (Exception e) {
            throw new DtLoaderException(e.getMessage(), e);
        } finally {
            closeResource(odps, odpsSourceDTO);
        }
        return SearchUtil.handleSearchAndLimit(tableList, queryDTO);
    }

    @Override
    public List<ColumnMetaDTO> getColumnMetaData(ISourceDTO iSource, SqlQueryDTO queryDTO) {
        OdpsSourceDTO odpsSourceDTO = (OdpsSourceDTO) iSource;
        beforeColumnQuery(odpsSourceDTO, queryDTO);
        List<ColumnMetaDTO> columnList = new ArrayList<>();
        Odps odps = null;
        try {
            odps = initOdps(odpsSourceDTO);
            Table table = odps.tables().get(queryDTO.getTableName());
            //获取非分区字段
            table.getSchema()
                    .getColumns().forEach(column -> {
                        ColumnMetaDTO columnMetaDTO = new ColumnMetaDTO();
                        columnMetaDTO.setKey(column.getName());
                        columnMetaDTO.setType(column.getTypeInfo().getTypeName());
                        columnMetaDTO.setComment(column.getComment());
                        columnList.add(columnMetaDTO);
                    });
            //获取分区字段
            table.getSchema()
                    .getPartitionColumns().forEach(partitionColumn -> {
                        ColumnMetaDTO columnMetaDTO = new ColumnMetaDTO();
                        columnMetaDTO.setKey(partitionColumn.getName());
                        columnMetaDTO.setType(partitionColumn.getTypeInfo().getTypeName());
                        columnMetaDTO.setComment(partitionColumn.getComment());
                        //设置为分区字段
                        columnMetaDTO.setPart(true);
                        columnList.add(columnMetaDTO);
                    });
        } catch (Exception e) {
            throw new DtLoaderException(e.getMessage(), e);
        } finally {
            closeResource(odps, odpsSourceDTO);
        }
        return columnList;
    }

    @Override
    public List<ColumnMetaDTO> getColumnMetaDataWithSql(ISourceDTO iSource, SqlQueryDTO queryDTO) {
        OdpsSourceDTO odpsSourceDTO = (OdpsSourceDTO) iSource;
        beforeQuery(odpsSourceDTO, queryDTO, true);
        List<ColumnMetaDTO> columnList = new ArrayList<>();
        Odps odps = null;
        try {
            odps = initOdps(odpsSourceDTO);
            Instance instance = runOdpsTask(odps, queryDTO);
            ResultSet records = SQLTask.getResultSet(instance);
            TableSchema tableSchema = records.getTableSchema();
            //获取非分区字段
            tableSchema.getColumns().forEach(column -> {
                ColumnMetaDTO columnMetaDTO = new ColumnMetaDTO();
                columnMetaDTO.setKey(column.getName());
                columnMetaDTO.setType(column.getTypeInfo().getTypeName());
                columnMetaDTO.setComment(column.getComment());
                columnList.add(columnMetaDTO);
            });
            //获取分区字段-应该不会走到这
            tableSchema.getPartitionColumns().forEach(partitionColumn -> {
                ColumnMetaDTO columnMetaDTO = new ColumnMetaDTO();
                columnMetaDTO.setKey(partitionColumn.getName());
                columnMetaDTO.setType(partitionColumn.getTypeInfo().getTypeName());
                columnMetaDTO.setComment(partitionColumn.getComment());
                //设置为分区字段
                columnMetaDTO.setPart(true);
                columnList.add(columnMetaDTO);
            });
        } catch (Exception e) {
            throw new DtLoaderException(String.format("SQL execute error,%s", e.getMessage()), e);
        } finally {
            closeResource(odps, odpsSourceDTO);
        }
        return columnList;
    }

    @Override
    public List<String> getColumnClassInfo(ISourceDTO iSource, SqlQueryDTO queryDTO) {
        OdpsSourceDTO odpsSourceDTO = (OdpsSourceDTO) iSource;
        Integer clearStatus = beforeColumnQuery(odpsSourceDTO, queryDTO);
        List<String> columnClassInfo = Lists.newArrayList();
        Odps odps = null;
        try {
            odps = initOdps(odpsSourceDTO);
            queryDTO.setSql("select * from " + queryDTO.getTableName());
            Instance instance = runOdpsTask(odps, queryDTO);
            ResultSet records = SQLTask.getResultSet(instance);
            TableSchema tableSchema = records.getTableSchema();
            for (Column recordColumn : tableSchema.getColumns()) {
                columnClassInfo.add(recordColumn.getTypeInfo().getTypeName());
            }
        } catch (Exception e) {
            throw new DtLoaderException(String.format("SQL execute error,%s", e.getMessage()), e);
        } finally {
            closeResource(odps, odpsSourceDTO);
        }
        return columnClassInfo;
    }

    @Override
    public List<List<Object>> getPreview(ISourceDTO iSource, SqlQueryDTO queryDTO) {
        OdpsSourceDTO odpsSourceDTO = (OdpsSourceDTO) iSource;
        List<List<Object>> dataList = new ArrayList<>();
        List<Object> columnMeta = Lists.newArrayList();
        //预览条数，默认100条
        Integer previewNum = queryDTO.getPreviewNum();
        if (StringUtils.isBlank(queryDTO.getTableName())) {
            return dataList;
        }
        Odps odps = null;
        try {
            odps = initOdps(odpsSourceDTO);
            Table t = odps.tables().get(queryDTO.getTableName());
            DefaultRecordReader recordReader;
            Map<String, String> partitionColumns = queryDTO.getPartitionColumns();
            if (MapUtils.isNotEmpty(partitionColumns)) {
                PartitionSpec partitionSpec = new PartitionSpec();
                Set<String> partSet = partitionColumns.keySet();
                for (String part : partSet) {
                    partitionSpec.set(part, partitionColumns.get(part));
                }
                recordReader = (DefaultRecordReader) t.read(partitionSpec, null, previewNum);
            } else {
                recordReader = (DefaultRecordReader) t.read(previewNum);
            }

            List<ColumnMetaDTO> metaData = getColumnMetaData(odpsSourceDTO, queryDTO);
            for (ColumnMetaDTO columnMetaDTO : metaData) {
                columnMeta.add(columnMetaDTO.getKey());
            }
            dataList.add(columnMeta);
            for (int i = 0; i < previewNum; i++) {
                List<String> result = recordReader.readRaw();
                if (CollectionUtils.isNotEmpty(result)) {
                    dataList.add(new ArrayList<>(result));
                }
            }
            return dataList;
        } catch (Exception e) {
            throw new DtLoaderException(e.getMessage(), e);
        } finally {
            closeResource(odps, odpsSourceDTO);
        }
    }

    @Override
    public List<Map<String, Object>> executeQuery(ISourceDTO iSource, SqlQueryDTO queryDTO) {
        OdpsSourceDTO odpsSourceDTO = (OdpsSourceDTO) iSource;
        beforeQuery(odpsSourceDTO, queryDTO, true);
        List<Map<String, Object>> result = Lists.newArrayList();
        Odps odps = null;
        try {
            odps = initOdps(odpsSourceDTO);
            Instance instance = runOdpsTask(odps, queryDTO);
            ResultSet records = SQLTask.getResultSet(instance);
            TableSchema tableSchema = records.getTableSchema();
            while (records.hasNext()) {
                Record record = records.next();
                Map<String, Object> row = Maps.newLinkedHashMap();
                for (Column recordColumn : tableSchema.getColumns()) {
                    String columnName = recordColumn.getName();
                    row.put(columnName, dealColumnType(record, recordColumn));
                }
                result.add(row);
            }
        } catch (Exception e) {
            throw new DtLoaderException(String.format("SQL execute error : %s", e.getMessage()), e);
        } finally {
            closeResource(odps, odpsSourceDTO);
        }
        return result;
    }

    /**
     * odps类型转换java类型
     *
     * @param record
     * @param recordColumn
     * @return
     */
    private Object dealColumnType(Record record, Column recordColumn) {
        OdpsType odpsType = recordColumn.getTypeInfo().getOdpsType();
        String columnName = recordColumn.getName();
        switch (odpsType) {
            case STRING:
                return record.getString(columnName);
            case DOUBLE:
                return record.getDouble(columnName);
            case BOOLEAN:
                return record.getBoolean(columnName);
            case DATE:
                return record.getDatetime(columnName);
            case DECIMAL:
                return record.getDecimal(columnName);
            case BIGINT:
                return record.getBigint(columnName);
            default:
                return record.get(columnName);
        }
    }

    @Override
    public Boolean executeSqlWithoutResultSet(ISourceDTO source, SqlQueryDTO queryDTO) {
        beforeQuery(source, queryDTO, true);
        OdpsSourceDTO odpsSourceDTO = (OdpsSourceDTO) source;
        Odps odps = null;
        boolean isSuccessful = false;
        try {
            odps = initOdps(odpsSourceDTO);
            Instance instance = runOdpsTask(odps, queryDTO);
            isSuccessful = instance.isSuccessful();
        } catch (Exception e) {
            throw new DtLoaderException(String.format("SQL execute error : %s", e.getMessage()), e);
        } finally {
            closeResource(odps, odpsSourceDTO);
        }

        return isSuccessful;
    }

    private void closeResource(Odps odps, OdpsSourceDTO odpsSourceDTO) {
        //归还对象
        if (BooleanUtils.isTrue(IS_OPEN_POOL.get()) && odps != null) {
            OdpsPool odpsPool = odpsManager.getConnection(odpsSourceDTO);
            odpsPool.returnResource(odps);
            IS_OPEN_POOL.remove();
        }
    }

    /**
     * 以任务的形式运行 SQL
     *
     * @param queryDTO
     * @return
     * @throws OdpsException
     */
    private Instance runOdpsTask(Odps odps, SqlQueryDTO queryDTO) throws OdpsException {
        // 查询 SQL 必须以 分号结尾
        String queryDTOSql = queryDTO.getSql();
        queryDTOSql = queryDTOSql.trim().endsWith(";") ? queryDTOSql : queryDTOSql.trim() + ";";
        Instance i = SQLTask.run(odps, queryDTOSql);
        i.waitForSuccess();
        return i;
    }

    @Override
    public String getTableMetaComment(ISourceDTO iSource, SqlQueryDTO queryDTO) {
        OdpsSourceDTO odpsSourceDTO = (OdpsSourceDTO) iSource;
        beforeColumnQuery(odpsSourceDTO, queryDTO);
        Odps odps = null;
        try {
            odps = initOdps(odpsSourceDTO);
            Table t = odps.tables().get(queryDTO.getTableName());
            return t.getComment();
        } catch (Exception e) {
            throw new DtLoaderException(e.getMessage(), e);
        } finally {
            closeResource(odps, odpsSourceDTO);
        }
    }

    protected Integer beforeQuery(ISourceDTO iSource, SqlQueryDTO queryDTO, boolean query) {
        // 查询 SQL 不能为空
        if (query && StringUtils.isBlank(queryDTO.getSql())) {
            throw new DtLoaderException("SQL is not null");
        }

        return ConnectionClearStatus.CLOSE.getValue();
    }

    protected Integer beforeColumnQuery(ISourceDTO iSource, SqlQueryDTO queryDTO) {
        OdpsSourceDTO odpsSourceDTO = (OdpsSourceDTO) iSource;
        Integer clearStatus = beforeQuery(odpsSourceDTO, queryDTO, false);
        if (queryDTO == null || StringUtils.isBlank(queryDTO.getTableName())) {
            throw new DtLoaderException("Query table name cannot be empty");
        }

        queryDTO.setColumns(CollectionUtils.isEmpty(queryDTO.getColumns()) ? Collections.singletonList("*") :
                queryDTO.getColumns());
        return clearStatus;
    }

    @Override
    public JobResult submitJob(ISourceDTO source, JobParam jobParam) {
        OdpsSourceDTO odpsSourceDTO = (OdpsSourceDTO) source;
        Odps odps = null;
        Instance instance;

        try {
            odps = initOdps(odpsSourceDTO);
            String[] sqls = jobParam.getSql().split(SPLIT);
            Job job = new Job();
            String guid = UUID.randomUUID().toString();
            String taskName = "query_task_" + Calendar.getInstance().getTimeInMillis();
            for (int i = 0; i < sqls.length; i++) {
                String sql = sqls[i];
                if (StringUtils.isEmpty(sql.trim())) {
                    continue;
                }

                if (!sql.endsWith(SPLIT)) {
                    sql = sql + SPLIT;
                }

                SQLTask task = new SQLTask();
                task.setName(String.format("%s_%s", taskName, i));
                task.setQuery(sql);
                task.setProperty("guid", guid);
                job.addTask(task);
            }

            instance = odps.instances().create(job);
        } catch (Exception e) {
            return JobResult.createErrorResult("submit odps job error, detail: " + e.getMessage());
        } finally {
            closeResource(odps, odpsSourceDTO);
        }
        return JobResult.createSuccessResult(instance.getId());
    }

    @Override
    public JobResult cancelJob(ISourceDTO source, JobParam jobParam) {
        OdpsSourceDTO odpsSourceDTO = (OdpsSourceDTO) source;
        Odps odps = null;
        try {
            odps = initOdps(odpsSourceDTO);
            Instance instance = getInstanceByJobId(odps, jobParam.getJobId());
            instance.stop();
        } catch (Exception e) {
            return JobResult.createErrorResult("cancel odps task error, " + e.getMessage());
        } finally {
            closeResource(odps, odpsSourceDTO);
        }

        JobResult jobResult = JobResult.newInstance(false);
        jobResult.setData("jobid", jobParam.getJobId());
        return jobResult;
    }

    @Override
    public String getJobStatus(ISourceDTO source, JobParam jobParam) {
        OdpsSourceDTO odpsSourceDTO = (OdpsSourceDTO) source;
        Odps odps = null;
        Instance.TaskStatus.Status taskStatus;

        try {
            odps = initOdps(odpsSourceDTO);
            Instance instance = getInstanceByJobId(odps, jobParam.getJobId());
            Map<String, Instance.TaskStatus> statusMap = instance.getTaskStatus();
            if (statusMap == null || statusMap.size() == 0) {
                throw new DtLoaderException("statusMap empty: " + jobParam.getJobId());
            }
            taskStatus = statusMap.entrySet().iterator().next().getValue().getStatus();
            if (taskStatus == null) {
                throw new DtLoaderException("can't find task status for task: " + jobParam.getJobId());
            }
        } catch (Exception e) {
            throw new DtLoaderException("get job status error ", e);
        } finally {
            closeResource(odps, odpsSourceDTO);
        }

        return JobStatus.getStatus(taskStatus.name());
    }

    @Override
    public String getJobLog(ISourceDTO source, JobParam jobParam) {
        OdpsSourceDTO odpsSourceDTO = (OdpsSourceDTO) source;
        Odps odps = null;
        //questions: 运行成功的job中间步骤可能也会有报错日志产出
//        if (!hasLog(source, jobParam.getJobId())) {
//            return "";
//        }

        try {
            odps = initOdps(odpsSourceDTO);
            Instance instance = getInstanceByJobId(odps, jobParam.getJobId());
            Map<String, String> taskResults = instance.getTaskResults();
            return taskResults.toString();
        } catch (Exception e) {
            throw new DtLoaderException("getLog error: " + jobParam.getJobId() + " msg:" + e.getMessage());
        } finally {
            closeResource(odps, odpsSourceDTO);
        }
    }

    private boolean hasLog(ISourceDTO source, String jobId) {
        String taskStatus = getJobStatus(source, JobParam.builder().jobId(jobId).build());
        return taskStatus.equals(Instance.TaskStatus.Status.FAILED.name());
    }

    @Override
    public Boolean judgeSlots(ISourceDTO source, JobParam jobParam) {
        return true;
    }

    /**
     * 根据jobId获取odps实例
     *
     * @param odps
     * @param jobId
     * @return
     */
    private Instance getInstanceByJobId(Odps odps, String jobId) {
        Instance instance = odps.instances().get(jobId);
        if (instance == null) {
            throw new DtLoaderException("can't find odps task: " + jobId);
        }

        return instance;
    }

}
