package com.dtstack.dtcenter.common.loader.inceptor.client;

import com.dtstack.dtcenter.common.loader.common.DtClassConsistent;
import com.dtstack.dtcenter.common.loader.common.enums.StoredType;
import com.dtstack.dtcenter.common.loader.common.utils.DBUtil;
import com.dtstack.dtcenter.common.loader.hadoop.hdfs.HdfsOperator;
import com.dtstack.dtcenter.common.loader.hadoop.util.KerberosConfigUtil;
import com.dtstack.dtcenter.common.loader.hadoop.util.KerberosLoginUtil;
import com.dtstack.dtcenter.common.loader.inceptor.InceptorConnFactory;
import com.dtstack.dtcenter.common.loader.inceptor.downloader.InceptorDownload;
import com.dtstack.dtcenter.common.loader.rdbms.AbsRdbmsClient;
import com.dtstack.dtcenter.common.loader.rdbms.ConnFactory;
import com.dtstack.dtcenter.loader.IDownloader;
import com.dtstack.dtcenter.loader.dto.ColumnMetaDTO;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.Table;
import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;
import com.dtstack.dtcenter.loader.dto.source.InceptorSourceDTO;
import com.dtstack.dtcenter.loader.enums.ConnectionClearStatus;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.dtstack.dtcenter.loader.kerberos.HadoopConfTool;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;

import java.security.PrivilegedAction;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

/**
 * inceptor client
 *
 * @author ：wangchuan
 * date：Created in 下午2:19 2021/5/6
 * company: www.dtstack.com
 */
@Slf4j
public class InceptorClient extends AbsRdbmsClient {

    // 创建库指定注释
    private static final String CREATE_DB_WITH_COMMENT = "create database if not exists %s comment '%s'";

    // 创建库
    private static final String CREATE_DB = "create database if not exists %s";

    // 模糊查询查询指定schema下的表
    private static final String TABLE_BY_SCHEMA_LIKE = "show tables in %s like '%s'";

    // 模糊查询database
    private static final String SHOW_DB_LIKE = "show databases like '%s'";

    private static final String SHOW_TABLE_SQL = "show tables %s";

    // 根据schema选表表名模糊查询
    private static final String SEARCH_SQL = " like '%s' ";

    @Override
    protected ConnFactory getConnFactory() {
        return new InceptorConnFactory();
    }

    @Override
    protected DataSourceType getSourceType() {
        return DataSourceType.INCEPTOR;
    }

    // 测试连通性超时时间。单位：秒
    private final static int TEST_CONN_TIMEOUT = 30;

    // metaStore 地址 key
    private final static String META_STORE_URIS_KEY = "hive.metastore.uris";

    // 是否启用 kerberos 认证
    private final static String META_STORE_SASL_ENABLED = "hive.metastore.sasl.enabled";

    // metaStore 地址 principal 地址
    private final static String META_STORE_KERBEROS_PRINCIPAL = "hive.metastore.kerberos.principal";

    @Override
    public List<String> getTableList(ISourceDTO sourceDTO, SqlQueryDTO queryDTO) {
        Integer clearStatus = beforeQuery(sourceDTO, queryDTO, false);
        InceptorSourceDTO inceptorSourceDTO = (InceptorSourceDTO) sourceDTO;
        StringBuilder constr = new StringBuilder();
        if (Objects.nonNull(queryDTO) && StringUtils.isNotBlank(queryDTO.getTableNamePattern())) {
            constr.append(String.format(SEARCH_SQL, addPercentSign(queryDTO.getTableNamePattern().trim())));
        }
        // 获取表信息需要通过show tables 语句
        String sql = String.format(SHOW_TABLE_SQL, constr.toString());
        Statement statement = null;
        ResultSet rs = null;
        List<String> tableList = new ArrayList<>();
        try {
            statement = inceptorSourceDTO.getConnection().createStatement();
            int maxLimit = 0;
            if (Objects.nonNull(queryDTO) && Objects.nonNull(queryDTO.getLimit())) {
                // 设置最大条数
                maxLimit = queryDTO.getLimit();
            }
            rs = statement.executeQuery(sql);
            int columnSize = rs.getMetaData().getColumnCount();
            int cnt = 0;
            while (rs.next()) {
                if(maxLimit > 0 && cnt >= maxLimit) {
                   break;
                }
                ++cnt;
                tableList.add(rs.getString(columnSize == 1 ? 1 : 2));
            }
        } catch (Exception e) {
            throw new DtLoaderException(String.format("get table exception,%s", e.getMessage()), e);
        } finally {
            DBUtil.closeDBResources(rs, statement, DBUtil.clearAfterGetConnection(inceptorSourceDTO, clearStatus));
        }
        return tableList;
    }

    @Override
    public List<String> getTableListBySchema(ISourceDTO sourceDTO, SqlQueryDTO queryDTO) {
        InceptorSourceDTO inceptorSourceDTO = (InceptorSourceDTO) sourceDTO;
        if (Objects.nonNull(queryDTO) && StringUtils.isNotBlank(queryDTO.getSchema())) {
            inceptorSourceDTO.setSchema(queryDTO.getSchema());
        }
        return getTableList(inceptorSourceDTO, queryDTO);
    }

    @Override
    public String getTableMetaComment(ISourceDTO sourceDTO, SqlQueryDTO queryDTO) {
        Integer clearStatus = beforeColumnQuery(sourceDTO, queryDTO);
        InceptorSourceDTO inceptorSourceDTO = (InceptorSourceDTO) sourceDTO;
        try {
            return getTableMetaComment(inceptorSourceDTO.getConnection(), queryDTO.getTableName());
        } finally {
            DBUtil.closeDBResources(null, null, DBUtil.clearAfterGetConnection(inceptorSourceDTO, clearStatus));
        }
    }

    /**
     * 获取表注释信息
     *
     * @param conn      数据源连接
     * @param tableName 表名
     * @return 表注释
     */
    private String getTableMetaComment(Connection conn, String tableName) {
        Statement statement = null;
        ResultSet resultSet = null;
        try {
            statement = conn.createStatement();
            resultSet = statement.executeQuery(String.format(DtClassConsistent.HadoopConfConsistent.DESCRIBE_EXTENDED
                    , tableName));
            while (resultSet.next()) {
                String columnName = resultSet.getString(1);
                if (StringUtils.isNotEmpty(columnName) && columnName.toLowerCase().contains(DtClassConsistent.HadoopConfConsistent.TABLE_INFORMATION)) {
                    String string = resultSet.getString(2);
                    if (StringUtils.isNotEmpty(string) && string.contains(DtClassConsistent.HadoopConfConsistent.HIVE_COMMENT)) {
                        String[] split = string.split(DtClassConsistent.HadoopConfConsistent.HIVE_COMMENT);
                        if (split.length > 1) {
                            return split[1].split("[,}\n]")[0].trim();
                        }
                    }
                }
            }
        } catch (Exception e) {
            throw new DtLoaderException(String.format("get table: %s's information error. Please contact the DBA to check the database、table information.",
                    tableName), e);
        } finally {
            DBUtil.closeDBResources(resultSet, statement, null);
        }
        return "";
    }

    private List<ColumnMetaDTO> getColumnMetaData(Connection conn, String tableName, Boolean filterPartitionColumns) {
        List<ColumnMetaDTO> columnMetaDTOS = new ArrayList<>();
        Statement stmt = null;
        ResultSet resultSet = null;

        try {
            stmt = conn.createStatement();
            resultSet = stmt.executeQuery(String.format(DtClassConsistent.HadoopConfConsistent.DESCRIBE_EXTENDED, tableName));
            while (resultSet.next()) {
                String dataType = resultSet.getString(DtClassConsistent.PublicConsistent.DATA_TYPE);
                String colName = resultSet.getString(DtClassConsistent.PublicConsistent.COL_NAME);
                if (StringUtils.isEmpty(dataType) || StringUtils.isBlank(colName)) {
                    break;
                }
                colName = colName.trim();
                ColumnMetaDTO metaDTO = new ColumnMetaDTO();
                metaDTO.setType(dataType.trim());
                metaDTO.setKey(colName);
                metaDTO.setComment(resultSet.getString(DtClassConsistent.PublicConsistent.COMMENT));

                if (colName.startsWith("#") || "Detailed Table Information".equals(colName)) {
                    break;
                }
                columnMetaDTOS.add(metaDTO);
            }

            DBUtil.closeDBResources(resultSet, null, null);
            resultSet = stmt.executeQuery(String.format(DtClassConsistent.HadoopConfConsistent.DESCRIBE_EXTENDED, tableName));
            boolean partBegin = false;
            while (resultSet.next()) {
                String colName = resultSet.getString(DtClassConsistent.PublicConsistent.COL_NAME).trim();

                if (colName.contains("# Partition Information")) {
                    partBegin = true;
                }

                if (colName.startsWith("#")) {
                    continue;
                }

                if ("Detailed Table Information".equals(colName)) {
                    break;
                }

                // 处理分区标志
                if (partBegin && !colName.contains("Partition Type")) {
                    Optional<ColumnMetaDTO> metaDTO =
                            columnMetaDTOS.stream().filter(meta -> colName.trim().equals(meta.getKey())).findFirst();
                    metaDTO.ifPresent(columnMetaDTO -> columnMetaDTO.setPart(true));
                } else if (colName.contains("Partition Type")) {
                    //分区字段结束
                    partBegin = false;
                }
            }

            return columnMetaDTOS.stream().filter(column -> !filterPartitionColumns || !column.getPart()).collect(Collectors.toList());
        } catch (SQLException e) {
            throw new DtLoaderException(String.format("Failed to get meta information for the fields of table :%s. Please contact the DBA to check the database table information,%s", tableName, e.getMessage()), e);
        } finally {
            DBUtil.closeDBResources(resultSet, stmt, null);
        }
    }

    @Override
    public List<ColumnMetaDTO> getColumnMetaData(ISourceDTO sourceDTO, SqlQueryDTO queryDTO) {
        Integer clearStatus = beforeColumnQuery(sourceDTO, queryDTO);
        InceptorSourceDTO inceptorSourceDTO = (InceptorSourceDTO) sourceDTO;
        try {
            return getColumnMetaData(inceptorSourceDTO.getConnection(), queryDTO.getTableName(), queryDTO.getFilterPartitionColumns());
        } finally {
            DBUtil.closeDBResources(null, null, DBUtil.clearAfterGetConnection(inceptorSourceDTO, clearStatus));
        }
    }

    @Override
    public Boolean testCon(ISourceDTO sourceDTO) {
        Future<Boolean> future = null;
        try {
            // 使用线程池的方式来控制连通超时
            Callable<Boolean> call = () -> testConnection(sourceDTO);
            future = executor.submit(call);
            // 如果在设定超时(以秒为单位)之内，还没得到连通性测试结果，则认为连通性测试连接超时，不继续阻塞
            return future.get(TEST_CONN_TIMEOUT, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            throw new DtLoaderException(String.format("Test connection timeout,%s", e.getMessage()), e);
        } catch (Exception e) {
            if (e instanceof DtLoaderException) {
                throw new DtLoaderException(e.getMessage(), e);
            }
            if (e.getCause() != null && e.getCause() instanceof DtLoaderException) {
                throw new DtLoaderException(e.getCause().getMessage(), e);
            }
            throw new DtLoaderException(e.getMessage(), e);
        } finally {
            if (Objects.nonNull(future)) {
                future.cancel(true);
            }
        }
    }

    private Boolean testConnection(ISourceDTO sourceDTO) {
        // 先校验数据源连接性
        Boolean testCon = super.testCon(sourceDTO);
        if (!testCon) {
            return Boolean.FALSE;
        }
        InceptorSourceDTO inceptorSourceDTO = (InceptorSourceDTO) sourceDTO;
        if (StringUtils.isBlank(inceptorSourceDTO.getDefaultFS())) {
            return Boolean.TRUE;
        }
        // 检查 metaStore 连通性
        checkMetaStoreConnect(inceptorSourceDTO.getMetaStoreUris(), inceptorSourceDTO.getKerberosConfig());
        return HdfsOperator.checkConnection(inceptorSourceDTO.getDefaultFS(), inceptorSourceDTO.getConfig(), inceptorSourceDTO.getKerberosConfig());
    }

    /**
     * 检查 metaStore 连通性
     *
     * @param metaStoreUris  metaStore 地址
     * @param kerberosConfig kerberos 配置
     */
    private void checkMetaStoreConnect(String metaStoreUris, Map<String, Object> kerberosConfig) {
        if (StringUtils.isBlank(metaStoreUris)) {
            return;
        }
        HiveConf hiveConf = new HiveConf();
        hiveConf.set(META_STORE_URIS_KEY, metaStoreUris);
        if (MapUtils.isNotEmpty(kerberosConfig)) {
            // metaStore kerberos 认证需要
            hiveConf.setBoolean(META_STORE_SASL_ENABLED, true);
            // 做两步兼容：先取 hive.metastore.kerberos.principal 的值，再取 principal，最后再取 keytab 中的第一个 principal
            String metaStorePrincipal = MapUtils.getString(kerberosConfig, META_STORE_KERBEROS_PRINCIPAL, MapUtils.getString(kerberosConfig, HadoopConfTool.PRINCIPAL));
            if (StringUtils.isBlank(metaStorePrincipal)) {
                String keytabPath = MapUtils.getString(kerberosConfig, HadoopConfTool.PRINCIPAL_FILE);
                metaStorePrincipal = KerberosConfigUtil.getPrincipals(keytabPath).get(0);
                if (StringUtils.isBlank(metaStorePrincipal)) {
                    throw new DtLoaderException("hive.metastore.kerberos.principal is not null...");
                }
            }
            log.info("hive.metastore.kerberos.principal:{}", metaStorePrincipal);
            hiveConf.set(META_STORE_KERBEROS_PRINCIPAL, metaStorePrincipal);
        }
        KerberosLoginUtil.loginWithUGI(kerberosConfig).doAs(
                (PrivilegedAction<Boolean>) () -> {
                    HiveMetaStoreClient client = null;
                    try {
                        client = new HiveMetaStoreClient(hiveConf);
                        client.getAllDatabases();
                    } catch (Exception e) {
                        throw new DtLoaderException(String.format("metastore connection failed.:%s", e.getMessage()));
                    } finally {
                        if (Objects.nonNull(client)) {
                            client.close();
                        }
                    }
                    return true;
                }
        );
    }

    @Override
    public IDownloader getDownloader(ISourceDTO sourceDTO, SqlQueryDTO queryDTO) throws Exception {
        InceptorDownload inceptorDownload = new InceptorDownload(getCon(sourceDTO), queryDTO.getSql());
        inceptorDownload.configure();
        return inceptorDownload;
    }

    /**
     * 处理hive分区信息和sql语句
     *
     * @param sqlQueryDTO 查询条件
     * @return 处理后的数据预览sql
     */
    @Override
    protected String dealSql(ISourceDTO sourceDTO, SqlQueryDTO sqlQueryDTO) {
        Map<String, String> partitions = sqlQueryDTO.getPartitionColumns();
        StringBuilder partSql = new StringBuilder();
        //拼接分区信息
        if (MapUtils.isNotEmpty(partitions)) {
            boolean check = true;
            partSql.append(" where ");
            Set<String> set = partitions.keySet();
            for (String column : set) {
                if (check) {
                    partSql.append(column).append("=").append(partitions.get(column));
                    check = false;
                } else {
                    partSql.append(" and ").append(column).append("=").append(partitions.get(column));
                }
            }
        }
        return "select * from " + sqlQueryDTO.getTableName() + partSql.toString();
    }

    @Override
    public List<ColumnMetaDTO> getPartitionColumn(ISourceDTO sourceDTO, SqlQueryDTO queryDTO) {
        List<ColumnMetaDTO> columnMetaDTOS = getColumnMetaData(sourceDTO, queryDTO);
        List<ColumnMetaDTO> partitionColumnMeta = new ArrayList<>();
        columnMetaDTOS.forEach(columnMetaDTO -> {
            if (columnMetaDTO.getPart()) {
                partitionColumnMeta.add(columnMetaDTO);
            }
        });
        return partitionColumnMeta;
    }

    @Override
    public Table getTable(ISourceDTO sourceDTO, SqlQueryDTO queryDTO) {
        Integer clearStatus = beforeQuery(sourceDTO, queryDTO, false);
        InceptorSourceDTO inceptorSourceDTO = (InceptorSourceDTO) sourceDTO;

        Table tableInfo = new Table();
        try {
            tableInfo.setName(queryDTO.getTableName());
            // 获取表注释
            tableInfo.setComment(getTableMetaComment(inceptorSourceDTO.getConnection(), queryDTO.getTableName()));
            // 处理字段信息
            tableInfo.setColumns(getColumnMetaData(inceptorSourceDTO.getConnection(), queryDTO.getTableName(), queryDTO.getFilterPartitionColumns()));
            // 获取表结构信息
            getTable(tableInfo, inceptorSourceDTO, queryDTO.getTableName());
        } catch (Exception e) {
            throw new DtLoaderException(String.format("SQL executed exception, %s", e.getMessage()), e);
        } finally {
            DBUtil.closeDBResources(null, null, DBUtil.clearAfterGetConnection(inceptorSourceDTO, clearStatus));
        }
        return tableInfo;
    }

    /**
     * 获取表结构信息
     *
     * @param tableInfo 表信息
     * @param inceptorSourceDTO      连接信息
     * @param tableName 表名
     */
    private void getTable(Table tableInfo, InceptorSourceDTO inceptorSourceDTO, String tableName) {
        List<Map<String, Object>> result = executeQuery(inceptorSourceDTO, SqlQueryDTO.builder().sql("desc formatted " + tableName).build(), ConnectionClearStatus.NORMAL.getValue());
        for (Map<String, Object> row : result) {
            // category和attribute不可为空
            if (StringUtils.isBlank(MapUtils.getString(row, "category")) || StringUtils.isBlank(MapUtils.getString(row, "attribute"))) {
                continue;
            }
            // 去空格处理
            String category = MapUtils.getString(row, "category").trim();
            String attribute = MapUtils.getString(row, "attribute").trim();

            if (StringUtils.containsIgnoreCase(category, "Location")) {
                tableInfo.setPath(attribute);
                continue;
            }

            if (StringUtils.containsIgnoreCase(category, "Type")) {
                tableInfo.setExternalOrManaged(attribute);
                continue;
            }

            if (StringUtils.containsIgnoreCase(category, "field.delim")) {
                tableInfo.setDelim(attribute);
                continue;
            }

            if (StringUtils.containsIgnoreCase(category, "Owner")) {
                tableInfo.setOwner(attribute);
                continue;
            }

            if (StringUtils.containsIgnoreCase(category, "CreateTime")) {
                tableInfo.setCreatedTime(attribute);
                continue;
            }

            if (StringUtils.containsIgnoreCase(category, "LastAccess")) {
                tableInfo.setLastAccess(attribute);
                continue;
            }

            if (StringUtils.containsIgnoreCase(category, "CreatedBy")) {
                tableInfo.setCreatedBy(attribute);
                continue;
            }

            if (StringUtils.containsIgnoreCase(category, "Database")) {
                tableInfo.setDb(attribute);
                continue;
            }

            if (StringUtils.containsIgnoreCase(category, "transactional")) {
                if (StringUtils.containsIgnoreCase(attribute, "true")) {
                    tableInfo.setIsTransTable(true);
                }
                continue;
            }

            if (StringUtils.containsIgnoreCase(category, "Type")) {
                tableInfo.setIsView(StringUtils.containsIgnoreCase(attribute, "VIEW"));
                continue;
            }

            if (StringUtils.containsIgnoreCase(category, "InputFormat")) {
                for (StoredType hiveStoredType : StoredType.values()) {
                    if (StringUtils.containsIgnoreCase(attribute, hiveStoredType.getInputFormatClass())) {
                        tableInfo.setStoreType(hiveStoredType.getValue());
                    }
                }
            }
        }
    }

    @Override
    protected String getCreateDatabaseSql(String dbName, String comment) {
        return StringUtils.isBlank(comment) ? String.format(CREATE_DB, dbName) : String.format(CREATE_DB_WITH_COMMENT, dbName, comment);
    }

    @Override
    public Boolean isDatabaseExists(ISourceDTO source, String dbName) {
        if (StringUtils.isBlank(dbName)) {
            throw new DtLoaderException("database name cannot be empty!");
        }
        return CollectionUtils.isNotEmpty(executeQuery(source, SqlQueryDTO.builder().sql(String.format(SHOW_DB_LIKE, dbName)).build()));
    }

    @Override
    public Boolean isTableExistsInDatabase(ISourceDTO source, String tableName, String dbName) {
        if (StringUtils.isBlank(dbName)) {
            throw new DtLoaderException("database name cannot be empty!");
        }
        return CollectionUtils.isNotEmpty(executeQuery(source, SqlQueryDTO.builder().sql(String.format(TABLE_BY_SCHEMA_LIKE, dbName, tableName)).build()));
    }

    @Override
    protected String addPercentSign(String str) {
        return "*" + str + "*";
    }
}
