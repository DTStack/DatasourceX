package com.dtstack.dtcenter.common.loader.rdbms.odps;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.aliyun.odps.Odps;
import com.aliyun.odps.Table;
import com.aliyun.odps.Tables;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.data.DefaultRecordReader;
import com.dtstack.dtcenter.common.enums.DataSourceType;
import com.dtstack.dtcenter.common.loader.rdbms.common.AbsRdbmsClient;
import com.dtstack.dtcenter.common.loader.rdbms.common.ConnFactory;
import com.dtstack.dtcenter.loader.dto.ColumnMetaDTO;
import com.dtstack.dtcenter.loader.dto.SourceDTO;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;

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




    @Override
    protected ConnFactory getConnFactory() {
        return new OdpsConnFactory();
    }

    @Override
    protected DataSourceType getSourceType() {
        return DataSourceType.MAXCOMPUTE;
    }

    @Override
    public Boolean testCon(SourceDTO source) {
        try {
            Odps odps = initOdps(JSON.parseObject(source.getConfig()));
            Tables tables = odps.tables();
            tables.iterator().hasNext();
            return true;
        } catch(Exception ex) {
            log.error("检查odps连接失败..{}",source, ex);
        }
        return false;
    }


    public static Odps initOdps(JSONObject odpsConfig) {
        Odps odps = initOdps(odpsConfig.getString(KEY_ODPS_SERVER), odpsConfig.getString(KEY_ACCESS_ID), odpsConfig.getString(KEY_ACCESS_KEY), odpsConfig.getString(KEY_PROJECT), odpsConfig.getString(PACKAGE_AUTHORIZED_PROJECT), odpsConfig.getString(KEY_ACCOUNT_TYPE));
        return odps;
    }

    public static Odps initOdps(String odpsServer, String accessId, String accessKey, String project, String packageAuthorizedProject, String accountType){
        if(StringUtils.isBlank(odpsServer)) {
            odpsServer = DEFAULT_ODPS_SERVER;
        }

        if(StringUtils.isBlank(accessId)) {
            throw new IllegalArgumentException("accessId is required");
        }

        if(StringUtils.isBlank(accessKey)) {
            throw new IllegalArgumentException("accessKey is required");
        }

        if(StringUtils.isBlank(project)) {
            throw new IllegalArgumentException("project is required");
        }

        String defaultProject;
        if(StringUtils.isBlank(packageAuthorizedProject)) {
            defaultProject = project;
        } else {
            defaultProject = packageAuthorizedProject;
        }

        if(StringUtils.isBlank(accountType)) {
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

    @Override
    public List<String> getTableList(SourceDTO source, SqlQueryDTO queryDTO) throws Exception {
            List<String> tableList = new ArrayList<>();
            Odps odps = initOdps(JSON.parseObject(source.getConfig()));
            odps.tables().forEach((Table table) -> tableList.add(table.getName()));
            return tableList;
    }

    @Override
    public List<ColumnMetaDTO> getColumnMetaData(SourceDTO source, SqlQueryDTO queryDTO) throws Exception {
        if (StringUtils.isBlank(queryDTO.getTableName())) {
            return null;
        }
        Odps odps = initOdps(JSON.parseObject(source.getConfig()));
        List<ColumnMetaDTO> columnList = new ArrayList<>();
        Table table = odps.tables().get(queryDTO.getTableName());
        table.getSchema()
                .getColumns().forEach(column -> {
                    ColumnMetaDTO columnMetaDTO = new ColumnMetaDTO();
                    columnMetaDTO.setKey(column.getName());
                    columnMetaDTO.setType(column.getTypeInfo().getTypeName());
                    columnList.add(columnMetaDTO);
                });

        return columnList;
    }

    @Override
    public List<List<Object>> getPreview(SourceDTO source, SqlQueryDTO queryDTO) {
        if (StringUtils.isBlank(queryDTO.getTableName())) {
            return null;
        }
        List<List<Object>> dataList = new ArrayList<>();
        try {
            Odps odps = initOdps(JSON.parseObject(source.getConfig()));
            Table t = odps.tables().get(queryDTO.getTableName());
            DefaultRecordReader recordReader = (DefaultRecordReader) t.read(3);
            for (int i = 0; i < 3; i++) {
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
    public String getTableMetaComment(SourceDTO source, SqlQueryDTO queryDTO) throws Exception {
        if (StringUtils.isBlank(queryDTO.getTableName())) {
            return "";
        }
        try {
            Odps odps = initOdps(JSON.parseObject(source.getConfig()));
            Table t = odps.tables().get(queryDTO.getTableName());
            return t.getComment();
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }
}
