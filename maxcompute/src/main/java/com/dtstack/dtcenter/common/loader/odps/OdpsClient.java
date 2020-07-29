package com.dtstack.dtcenter.common.loader.odps;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.aliyun.odps.Column;
import com.aliyun.odps.Instance;
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
import com.dtstack.dtcenter.common.enums.DataSourceType;
import com.dtstack.dtcenter.common.exception.DBErrorCode;
import com.dtstack.dtcenter.common.exception.DtCenterDefException;
import com.dtstack.dtcenter.common.loader.common.AbsRdbmsClient;
import com.dtstack.dtcenter.common.loader.common.ConnFactory;
import com.dtstack.dtcenter.loader.dto.ColumnMetaDTO;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;
import com.dtstack.dtcenter.loader.dto.source.OdpsSourceDTO;
import com.dtstack.dtcenter.loader.enums.ConnectionClearStatus;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 17:47 2020/1/7
 * @Description：ODPS 客户端
 */
@Slf4j
public class OdpsClient extends AbsRdbmsClient {
    private final static String DEFAULT_ODPS_SERVER = "http://service.odps.aliyun.com/api";

    private final static String KEY_ODPS_SERVER = "endPoint";

    private final static String KEY_ACCESS_ID = "accessId";

    private final static String KEY_ACCESS_KEY = "accessKey";

    private static final String KEY_PROJECT = "project";

    private static final String PACKAGE_AUTHORIZED_PROJECT = "packageAuthorizedProject";

    private final static String KEY_ACCOUNT_TYPE = "accountType";

    private static final String DEFAULT_ACCOUNT_TYPE = "aliyun";

    private static final String ODPS_KEY = "endPoint:%s,accessId:%s,accessKey:%s,project:%s,packageAuthorizedProject:%s,accountType:%s";

    private static final String cacheMethodName = "getIsCache";

    private static ConcurrentHashMap<String, Odps> odpsDataSources = new ConcurrentHashMap<>();

    @Override
    protected ConnFactory getConnFactory() {
        return new OdpsConnFactory();
    }

    @Override
    protected DataSourceType getSourceType() {
        return DataSourceType.MAXCOMPUTE;
    }

    @Override
    public Boolean testCon(ISourceDTO iSource) {
        OdpsSourceDTO odpsSourceDTO = (OdpsSourceDTO) iSource;
        try {
            Odps odps = initOdps(odpsSourceDTO);
            Tables tables = odps.tables();
            tables.iterator().hasNext();
            return true;
        } catch (Exception ex) {
            log.error("检查odps连接失败..{}", odpsSourceDTO, ex);
        }
        return false;
    }


    public static Odps initOdps(OdpsSourceDTO odpsSourceDTO) {
        boolean isCache = false;
        //适配之前的版本，判断ISourceDTO类中有无获取isCache字段的方法
        Method[] methods = ISourceDTO.class.getDeclaredMethods();
        for (Method method:methods) {
            if (cacheMethodName.equals(method.getName())) {
                isCache = odpsSourceDTO.getIsCache();
                break;
            }
        }
        JSONObject odpsConfig = JSON.parseObject(odpsSourceDTO.getConfig());
        //不开启缓存
        if (!isCache) {
            return initOdps(odpsConfig.getString(KEY_ODPS_SERVER), odpsConfig.getString(KEY_ACCESS_ID),
                    odpsConfig.getString(KEY_ACCESS_KEY), odpsConfig.getString(KEY_PROJECT),
                    odpsConfig.getString(PACKAGE_AUTHORIZED_PROJECT), odpsConfig.getString(KEY_ACCOUNT_TYPE));
        }
        //开启缓存
        String primaryKey = getPrimaryKey(odpsConfig.getString(KEY_ODPS_SERVER), odpsConfig.getString(KEY_ACCESS_ID),
                odpsConfig.getString(KEY_ACCESS_KEY), odpsConfig.getString(KEY_PROJECT),
                odpsConfig.getString(PACKAGE_AUTHORIZED_PROJECT), odpsConfig.getString(KEY_ACCOUNT_TYPE));
        Odps odps = odpsDataSources.get(primaryKey);
        if (odps == null) {
            synchronized (OdpsClient.class) {
                odps = odpsDataSources.get(primaryKey);
                if (odps == null) {
                    odps = initOdps(odpsConfig.getString(KEY_ODPS_SERVER), odpsConfig.getString(KEY_ACCESS_ID),
                            odpsConfig.getString(KEY_ACCESS_KEY), odpsConfig.getString(KEY_PROJECT),
                            odpsConfig.getString(PACKAGE_AUTHORIZED_PROJECT), odpsConfig.getString(KEY_ACCOUNT_TYPE));
                    odpsDataSources.put(primaryKey, odps);
                }
            }
        }
        return odps;
    }

    public static Odps initOdps(String odpsServer, String accessId, String accessKey, String project,
                                String packageAuthorizedProject, String accountType) {
        if (StringUtils.isBlank(odpsServer)) {
            odpsServer = DEFAULT_ODPS_SERVER;
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
            accountType = DEFAULT_ACCOUNT_TYPE;
        }

        Account account = null;
        if (accountType.equalsIgnoreCase(DEFAULT_ACCOUNT_TYPE)) {
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

    /**
     * 获取唯一key
     * @param odpsServer
     * @param accessId
     * @param accessKey
     * @param project
     * @param packageAuthorizedProject
     * @param accountType
     * @return
     */
    private static String getPrimaryKey (String odpsServer, String accessId, String accessKey, String project,
                                         String packageAuthorizedProject, String accountType) {
        return String.format(ODPS_KEY, odpsServer, accessId, accessKey, project, packageAuthorizedProject, accountType);
    }

    @Override
    public List<String> getTableList(ISourceDTO iSource, SqlQueryDTO queryDTO) throws Exception {
        OdpsSourceDTO odpsSourceDTO = (OdpsSourceDTO) iSource;
        beforeQuery(odpsSourceDTO, queryDTO, false);
        List<String> tableList = new ArrayList<>();
        Odps odps = initOdps(odpsSourceDTO);
        odps.tables().forEach((Table table) -> tableList.add(table.getName()));
        return tableList;
    }

    @Override
    public List<ColumnMetaDTO> getColumnMetaData(ISourceDTO iSource, SqlQueryDTO queryDTO) throws Exception {
        OdpsSourceDTO odpsSourceDTO = (OdpsSourceDTO) iSource;
        beforeColumnQuery(odpsSourceDTO, queryDTO);
        List<ColumnMetaDTO> columnList = new ArrayList<>();
        Odps odps = initOdps(odpsSourceDTO);
        Table table = odps.tables().get(queryDTO.getTableName());
        //获取非分区字段
        table.getSchema()
                .getColumns().forEach(column -> {
            ColumnMetaDTO columnMetaDTO = new ColumnMetaDTO();
            columnMetaDTO.setKey(column.getName());
            columnMetaDTO.setType(column.getTypeInfo().getTypeName());
            columnList.add(columnMetaDTO);
        });
        //获取分区字段
        table.getSchema()
                .getPartitionColumns().forEach(partitionColumn -> {
            ColumnMetaDTO columnMetaDTO = new ColumnMetaDTO();
            columnMetaDTO.setKey(partitionColumn.getName());
            columnMetaDTO.setType(partitionColumn.getTypeInfo().getTypeName());
            //设置为分区字段
            columnMetaDTO.setPart(true);
            columnList.add(columnMetaDTO);
        });
        return columnList;
    }

    @Override
    public List<ColumnMetaDTO> getColumnMetaDataWithSql(ISourceDTO iSource, SqlQueryDTO queryDTO) throws Exception {
        OdpsSourceDTO odpsSourceDTO = (OdpsSourceDTO) iSource;
        beforeQuery(odpsSourceDTO, queryDTO, true);
        List<ColumnMetaDTO> columnList = new ArrayList<>();
        try {
            Instance instance = runOdpsTask(odpsSourceDTO, queryDTO);
            ResultSet records = SQLTask.getResultSet(instance);
            TableSchema tableSchema = records.getTableSchema();
            //获取非分区字段
            tableSchema.getColumns().forEach(column -> {
                ColumnMetaDTO columnMetaDTO = new ColumnMetaDTO();
                columnMetaDTO.setKey(column.getName());
                columnMetaDTO.setType(column.getTypeInfo().getTypeName());
                columnList.add(columnMetaDTO);
            });
            //获取分区字段-应该不会走到这
            tableSchema.getPartitionColumns().forEach(partitionColumn -> {
                ColumnMetaDTO columnMetaDTO = new ColumnMetaDTO();
                columnMetaDTO.setKey(partitionColumn.getName());
                columnMetaDTO.setType(partitionColumn.getTypeInfo().getTypeName());
                //设置为分区字段
                columnMetaDTO.setPart(true);
                columnList.add(columnMetaDTO);
            });
        } catch (OdpsException e) {
            throw new DtCenterDefException(DBErrorCode.SQL_EXE_EXCEPTION, e);
        }
        return columnList;
    }

    @Override
    public List<String> getColumnClassInfo(ISourceDTO iSource, SqlQueryDTO queryDTO) throws Exception {
        OdpsSourceDTO odpsSourceDTO = (OdpsSourceDTO) iSource;
        Integer clearStatus = beforeColumnQuery(odpsSourceDTO, queryDTO);
        List<String> columnClassInfo = Lists.newArrayList();
        try {
            queryDTO.setSql("select * from " + queryDTO.getTableName());
            Instance instance = runOdpsTask(odpsSourceDTO, queryDTO);
            ResultSet records = SQLTask.getResultSet(instance);
            TableSchema tableSchema = records.getTableSchema();
            for (Column recordColumn : tableSchema.getColumns()) {
                columnClassInfo.add(recordColumn.getTypeInfo().getTypeName());
            }
        } catch (OdpsException e) {
            throw new DtCenterDefException(DBErrorCode.SQL_EXE_EXCEPTION, e);
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
        try {
            Odps odps = initOdps(odpsSourceDTO);
            Table t = odps.tables().get(queryDTO.getTableName());
            DefaultRecordReader recordReader;
            Map<String, String> partitionColumns = queryDTO.getPartitionColumns();
            if (MapUtils.isNotEmpty(partitionColumns)){
                PartitionSpec partitionSpec = new PartitionSpec();
                Set<String> partSet = partitionColumns.keySet();
                for (String part:partSet){
                    partitionSpec.set(part, partitionColumns.get(part));
                }
                recordReader = (DefaultRecordReader) t.read(partitionSpec, null, previewNum);
            }else {
                recordReader = (DefaultRecordReader) t.read(previewNum);
            }

            List<ColumnMetaDTO> metaData = getColumnMetaData(odpsSourceDTO, queryDTO);
            for (ColumnMetaDTO columnMetaDTO:metaData){
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
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    @Override
    public List<Map<String, Object>> executeQuery(ISourceDTO iSource, SqlQueryDTO queryDTO) throws Exception {
        OdpsSourceDTO odpsSourceDTO = (OdpsSourceDTO) iSource;
        beforeQuery(odpsSourceDTO, queryDTO, true);
        List<Map<String, Object>> result = Lists.newArrayList();
        try {
            Instance instance = runOdpsTask(odpsSourceDTO, queryDTO);
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
        } catch (OdpsException e) {
            throw new DtCenterDefException(DBErrorCode.SQL_EXE_EXCEPTION, e);
        }

        return result;
    }

    //odps类型转换java类型
    private Object dealColumnType(Record record, Column recordColumn) {
        OdpsType odpsType = recordColumn.getTypeInfo().getOdpsType();
        String columnName = recordColumn.getName();
        switch (odpsType){
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
    public Boolean executeSqlWithoutResultSet(ISourceDTO source, SqlQueryDTO queryDTO) throws Exception {
        beforeQuery(source, queryDTO, true);
        Instance instance = runOdpsTask(source, queryDTO);
        return instance.isSuccessful();
    }

    /**
     * 以任务的形式运行 SQL
     * @param iSource
     * @param queryDTO
     * @return
     * @throws OdpsException
     */
    private Instance runOdpsTask(ISourceDTO iSource, SqlQueryDTO queryDTO) throws OdpsException {
        OdpsSourceDTO odpsSourceDTO = (OdpsSourceDTO) iSource;
        Odps odps = initOdps(odpsSourceDTO);
        // 查询 SQL 必须以 分号结尾
        String queryDTOSql = queryDTO.getSql();
        queryDTOSql = queryDTOSql.trim().endsWith(";") ? queryDTOSql : queryDTOSql.trim() + ";";
        Instance i = SQLTask.run(odps, queryDTOSql);
        i.waitForSuccess();
        return i;
    }

    @Override
    public String getTableMetaComment(ISourceDTO iSource, SqlQueryDTO queryDTO) throws Exception {
        OdpsSourceDTO odpsSourceDTO = (OdpsSourceDTO) iSource;
        beforeColumnQuery(odpsSourceDTO, queryDTO);
        try {
            Odps odps = initOdps(odpsSourceDTO);
            Table t = odps.tables().get(queryDTO.getTableName());
            return t.getComment();
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    @Override
    protected Integer beforeQuery(ISourceDTO iSource, SqlQueryDTO queryDTO, boolean query) throws Exception {
        // 查询 SQL 不能为空
        if (query && StringUtils.isBlank(queryDTO.getSql())) {
            throw new DtLoaderException("查询 SQL 不能为空");
        }

        return ConnectionClearStatus.CLOSE.getValue();
    }

    @Override
    protected Integer beforeColumnQuery(ISourceDTO iSource, SqlQueryDTO queryDTO) throws Exception {
        OdpsSourceDTO odpsSourceDTO = (OdpsSourceDTO) iSource;
        Integer clearStatus = beforeQuery(odpsSourceDTO, queryDTO, false);
        if (queryDTO == null || StringUtils.isBlank(queryDTO.getTableName())) {
            throw new DtLoaderException("查询 表名称 不能为空");
        }

        queryDTO.setColumns(CollectionUtils.isEmpty(queryDTO.getColumns()) ? Collections.singletonList("*") :
                queryDTO.getColumns());
        return clearStatus;
    }
}
