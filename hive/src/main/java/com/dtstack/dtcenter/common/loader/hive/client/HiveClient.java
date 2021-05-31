package com.dtstack.dtcenter.common.loader.hive.client;

import com.dtstack.dtcenter.common.loader.common.DtClassConsistent;
import com.dtstack.dtcenter.common.loader.common.enums.StoredType;
import com.dtstack.dtcenter.common.loader.common.utils.DBUtil;
import com.dtstack.dtcenter.common.loader.common.utils.ReflectUtil;
import com.dtstack.dtcenter.common.loader.hadoop.hdfs.HadoopConfUtil;
import com.dtstack.dtcenter.common.loader.hadoop.hdfs.HdfsOperator;
import com.dtstack.dtcenter.common.loader.hadoop.util.KerberosLoginUtil;
import com.dtstack.dtcenter.common.loader.hive.HiveConnFactory;
import com.dtstack.dtcenter.common.loader.hive.downloader.HiveORCDownload;
import com.dtstack.dtcenter.common.loader.hive.downloader.HiveParquetDownload;
import com.dtstack.dtcenter.common.loader.hive.downloader.HiveTextDownload;
import com.dtstack.dtcenter.common.loader.rdbms.AbsRdbmsClient;
import com.dtstack.dtcenter.common.loader.rdbms.ConnFactory;
import com.dtstack.dtcenter.loader.IDownloader;
import com.dtstack.dtcenter.loader.dto.ColumnMetaDTO;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.Table;
import com.dtstack.dtcenter.loader.dto.source.HiveSourceDTO;
import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;
import com.dtstack.dtcenter.loader.enums.ConnectionClearStatus;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.jetbrains.annotations.NotNull;

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
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 17:06 2020/1/7
 * @Description：Hive 连接
 */
@Slf4j
public class HiveClient extends AbsRdbmsClient {

    // 获取正在使用数据库
    private static final String CURRENT_DB = "select current_database()";

    // 测试连通性超时时间。单位：秒
    private final static int TEST_CONN_TIMEOUT = 30;

    // 创建库指定注释
    private static final String CREATE_DB_WITH_COMMENT = "create database if not exists %s comment '%s'";

    // 创建库
    private static final String CREATE_DB = "create database if not exists %s";

    // 模糊查询查询指定schema下的表
    private static final String TABLE_BY_SCHEMA_LIKE = "show tables in %s like '%s'";

    // 模糊查询database
    private static final String SHOW_DB_LIKE = "show databases like '%s'";

    // null 名称的字段名
    private static final String NULL_COLUMN = "null";

    @Override
    protected ConnFactory getConnFactory() {
        return new HiveConnFactory();
    }

    @Override
    protected DataSourceType getSourceType() {
        return DataSourceType.HIVE;
    }

    @Override
    public List<String> getTableList(ISourceDTO iSource, SqlQueryDTO queryDTO) {
        Integer clearStatus = beforeQuery(iSource, queryDTO, false);
        HiveSourceDTO hiveSourceDTO = (HiveSourceDTO) iSource;
        // 获取表信息需要通过show tables 语句
        String sql = "show tables";
        Statement statement = null;
        ResultSet rs = null;
        List<String> tableList = new ArrayList<>();
        try {
            statement = hiveSourceDTO.getConnection().createStatement();
            rs = statement.executeQuery(sql);
            int columnSize = rs.getMetaData().getColumnCount();
            while (rs.next()) {
                tableList.add(rs.getString(columnSize == 1 ? 1 : 2));
            }
        } catch (Exception e) {
            throw new DtLoaderException(String.format("get table exception,%", e.getMessage()), e);
        } finally {
            DBUtil.closeDBResources(rs, statement, DBUtil.clearAfterGetConnection(hiveSourceDTO, clearStatus));
        }
        return tableList;
    }

    @Override
    public List<String> getTableListBySchema(ISourceDTO source, SqlQueryDTO queryDTO) {
        HiveSourceDTO hiveSourceDTO = (HiveSourceDTO) source;
        if (Objects.nonNull(queryDTO) && StringUtils.isNotBlank(queryDTO.getSchema())) {
            hiveSourceDTO.setSchema(queryDTO.getSchema());
        }
        return getTableList(hiveSourceDTO, queryDTO);
    }

    @Override
    public String getTableMetaComment(ISourceDTO iSource, SqlQueryDTO queryDTO) {
        Integer clearStatus = beforeColumnQuery(iSource, queryDTO);
        HiveSourceDTO hiveSourceDTO = (HiveSourceDTO) iSource;
        try {
            return getTableMetaComment(hiveSourceDTO.getConnection(), queryDTO.getTableName());
        } finally {
            DBUtil.closeDBResources(null, null, DBUtil.clearAfterGetConnection(hiveSourceDTO, clearStatus));
        }
    }

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
                    // 兼容两种数据结构 Comment: comment=
                    String string = resultSet.getString(2);
                    if (StringUtils.isNotEmpty(string) && string.contains(DtClassConsistent.HadoopConfConsistent.HIVE_COMMENT)) {
                        String[] split = string.split(DtClassConsistent.HadoopConfConsistent.HIVE_COMMENT);
                        if (split.length > 1) {
                            return split[1].split(",|}|\n")[0].trim();
                        }
                    }

                    if (StringUtils.isNotEmpty(string) && string.contains(DtClassConsistent.HadoopConfConsistent.COMMENT_WITH_COLON)) {
                        String[] split = string.split(DtClassConsistent.HadoopConfConsistent.COMMENT_WITH_COLON);
                        if (split.length > 1) {
                            return split[1].split(",|}|\n")[0].trim();
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

    @Override
    public List<ColumnMetaDTO> getColumnMetaData(ISourceDTO iSource, SqlQueryDTO queryDTO) {
        Integer clearStatus = beforeColumnQuery(iSource, queryDTO);
        HiveSourceDTO hiveSourceDTO = (HiveSourceDTO) iSource;
        try {
            return getColumnMetaData(hiveSourceDTO.getConnection(), queryDTO.getTableName(), queryDTO.getFilterPartitionColumns());
        } finally {
            DBUtil.closeDBResources(null, null, DBUtil.clearAfterGetConnection(hiveSourceDTO, clearStatus));
        }
    }

    private List<ColumnMetaDTO> getColumnMetaData(Connection conn, String tableName, Boolean filterPartitionColumns) {
        List<ColumnMetaDTO> columnMetaDTOS = new ArrayList<>();
        Statement stmt = null;
        ResultSet resultSet = null;

        try {
            stmt = conn.createStatement();
            resultSet = stmt.executeQuery("desc extended " + tableName);
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
            resultSet = stmt.executeQuery("desc extended " + tableName);
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
                    if (metaDTO.isPresent()) {
                        metaDTO.get().setPart(true);
                    }
                } else if (colName.contains("Partition Type")) {
                    //分区字段结束
                    partBegin = false;
                }
            }

            return columnMetaDTOS.stream().filter(column -> !filterPartitionColumns || !column.getPart()).collect(Collectors.toList());
        } catch (SQLException e) {
            throw new DtLoaderException(String.format("Failed to get meta information for the fields of table :%s. Please contact the DBA to check the database table information.",
                    tableName), e);
        } finally {
            DBUtil.closeDBResources(resultSet, stmt, null);
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
            throw new DtLoaderException(String.format("Test connect timeout！,%s", e.getMessage()), e);
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

    private Boolean testConnection(ISourceDTO iSource) {
        // 先校验数据源连接性
        Boolean testCon = super.testCon(iSource);
        if (!testCon) {
            return Boolean.FALSE;
        }
        HiveSourceDTO hiveSourceDTO = (HiveSourceDTO) iSource;
        if (StringUtils.isBlank(hiveSourceDTO.getDefaultFS())) {
            return Boolean.TRUE;
        }

        return HdfsOperator.checkConnection(hiveSourceDTO.getDefaultFS(), hiveSourceDTO.getConfig(), hiveSourceDTO.getKerberosConfig());
    }

    @Override
    public IDownloader getDownloader(ISourceDTO iSource, SqlQueryDTO queryDTO) {
        HiveSourceDTO hiveSourceDTO = (HiveSourceDTO) iSource;
        List<Map<String, Object>> list = executeQuery(hiveSourceDTO, SqlQueryDTO.builder().sql("desc formatted " + queryDTO.getTableName()).build());
        // 获取表路径、字段分隔符、存储方式
        String tableLocation = null;
        String fieldDelimiter = "\001";
        String storageMode = null;
        for (Map<String, Object> map : list) {
            String colName = MapUtils.getString(map, "col_name");
            String dataType = MapUtils.getString(map, "data_type");
            if (colName.contains("Location") && StringUtils.isNotBlank(dataType)) {
                tableLocation = dataType.trim();
                continue;
            }

            if (colName.contains("InputFormat")) {
                storageMode = dataType;
                continue;
            }

            if (colName.contains("field.delim")) {
                fieldDelimiter = dataType;
                break;
            }

            if (Objects.nonNull(dataType) && dataType.contains("field.delim")) {
                fieldDelimiter = MapUtils.getString(map, "comment");
                break;
            }
        }
        // 普通字段集合
        ArrayList<String> columnNames = new ArrayList<>();
        // 分区字段集合
        ArrayList<String> partitionColumns = new ArrayList<>();
        // 获取所有字段信息
        List<ColumnMetaDTO> columnMetaData = getColumnMetaData(hiveSourceDTO, queryDTO);
        // 查询的字段列表，支持按字段获取数据
        List<String> columns = queryDTO.getColumns();
        // 需要的字段索引（包括分区字段索引）
        List<Integer> needIndex = Lists.newArrayList();

        for (ColumnMetaDTO columnMetaDatum : columnMetaData) {
            // 非分区字段
            if (columnMetaDatum.getPart()) {
                partitionColumns.add(columnMetaDatum.getKey());
                continue;
            }
            columnNames.add(columnMetaDatum.getKey());
        }

        // columns字段不为空且不包含*时获取指定字段的数据
        if (CollectionUtils.isNotEmpty(columns) && !columns.contains("*")) {
            // 保证查询字段的顺序!
            for (String column : columns) {
                if (NULL_COLUMN.equalsIgnoreCase(column)) {
                    needIndex.add(Integer.MAX_VALUE);
                    continue;
                }
                // 判断查询字段是否存在
                boolean check = false;
                for (int j = 0; j < columnMetaData.size(); j++) {
                    if (column.equalsIgnoreCase(columnMetaData.get(j).getKey())) {
                        needIndex.add(j);
                        check = true;
                        break;
                    }
                }
                if (!check) {
                    throw new DtLoaderException("The query field does not exist! Field name：" + column);
                }
            }
        }

        // 校验高可用配置
        if (StringUtils.isBlank(hiveSourceDTO.getDefaultFS()) || !hiveSourceDTO.getDefaultFS().matches(DtClassConsistent.HadoopConfConsistent.DEFAULT_FS_REGEX)) {
            throw new DtLoaderException("defaultFS incorrect format");
        }
        Configuration conf = HadoopConfUtil.getHdfsConf(hiveSourceDTO.getDefaultFS(), hiveSourceDTO.getConfig(), hiveSourceDTO.getKerberosConfig());

        String finalStorageMode = storageMode;
        Configuration finalConf = conf;
        String finalTableLocation = tableLocation;
        String finalFieldDelimiter = fieldDelimiter;
        return KerberosLoginUtil.loginWithUGI(hiveSourceDTO.getKerberosConfig()).doAs(
                (PrivilegedAction<IDownloader>) () -> {
                    try {
                        return createDownloader(finalStorageMode, finalConf, finalTableLocation, columnNames, finalFieldDelimiter, partitionColumns, needIndex, queryDTO.getPartitionColumns(), hiveSourceDTO.getKerberosConfig());
                    } catch (Exception e) {
                        throw new DtLoaderException(String.format("create downloader exception,%s", e.getMessage()), e);
                    }
                }
        );
    }

    /**
     * 根据存储格式创建对应的hiveDownloader
     *
     * @param storageMode
     * @param conf
     * @param tableLocation
     * @param columnNames
     * @param fieldDelimiter
     * @param partitionColumns
     * @return
     * @throws Exception
     */
    private @NotNull IDownloader createDownloader(String storageMode, Configuration conf, String tableLocation, ArrayList<String> columnNames, String fieldDelimiter, ArrayList<String> partitionColumns, List<Integer> needIndex, Map<String, String> filterPartitions, Map<String, Object> kerberosConfig) throws Exception {
        // 根据存储格式创建对应的hiveDownloader
        if (StringUtils.isBlank(storageMode)) {
            throw new DtLoaderException("Hive table reads for this storage type are not supported");
        }

        if (storageMode.contains("Text")) {
            HiveTextDownload hiveTextDownload = new HiveTextDownload(conf, tableLocation, columnNames, fieldDelimiter, partitionColumns, filterPartitions, needIndex, kerberosConfig);
            hiveTextDownload.configure();
            return hiveTextDownload;
        }

        if (storageMode.contains("Orc")) {
            HiveORCDownload hiveORCDownload = new HiveORCDownload(conf, tableLocation, columnNames, partitionColumns, needIndex, kerberosConfig);
            hiveORCDownload.configure();
            return hiveORCDownload;
        }

        if (storageMode.contains("Parquet")) {
            HiveParquetDownload hiveParquetDownload = new HiveParquetDownload(conf, tableLocation, columnNames, partitionColumns, needIndex, filterPartitions, kerberosConfig);
            hiveParquetDownload.configure();
            return hiveParquetDownload;
        }

        throw new DtLoaderException("Hive table reads for this storage type are not supported");
    }

    /**
     * 处理hive分区信息和sql语句
     *
     * @param sqlQueryDTO 查询条件
     * @return
     */
    @Override
    protected String dealSql(ISourceDTO iSourceDTO, SqlQueryDTO sqlQueryDTO) {
        Map<String, String> partitions = sqlQueryDTO.getPartitionColumns();
        StringBuilder partSql = new StringBuilder();
        //拼接分区信息
        if (MapUtils.isNotEmpty(partitions)) {
            boolean check = true;
            partSql.append(" where ");
            Set<String> set = partitions.keySet();
            for (String column : set) {
                if (check) {
                    partSql.append(column + "=").append(partitions.get(column));
                    check = false;
                } else {
                    partSql.append(" and ").append(column + "=").append(partitions.get(column));
                }
            }
        }

        return "select * from " + sqlQueryDTO.getTableName() + partSql.toString();
    }

    @Override
    public List<ColumnMetaDTO> getPartitionColumn(ISourceDTO source, SqlQueryDTO queryDTO) {
        List<ColumnMetaDTO> columnMetaDTOS = getColumnMetaData(source, queryDTO);
        List<ColumnMetaDTO> partitionColumnMeta = new ArrayList<>();
        columnMetaDTOS.forEach(columnMetaDTO -> {
            if (columnMetaDTO.getPart()) {
                partitionColumnMeta.add(columnMetaDTO);
            }
        });
        return partitionColumnMeta;
    }

    @Override
    public Table getTable(ISourceDTO source, SqlQueryDTO queryDTO) {
        Integer clearStatus = beforeColumnQuery(source, queryDTO);
        HiveSourceDTO hiveSourceDTO = (HiveSourceDTO) source;

        Table tableInfo = new Table();
        try {
            tableInfo.setName(queryDTO.getTableName());
            // 获取表注释
            tableInfo.setComment(getTableMetaComment(hiveSourceDTO.getConnection(), queryDTO.getTableName()));
            // 处理字段信息
            tableInfo.setColumns(getColumnMetaData(hiveSourceDTO.getConnection(), queryDTO.getTableName(), queryDTO.getFilterPartitionColumns()));
            // 获取表结构信息
            getTable(tableInfo, hiveSourceDTO, queryDTO.getTableName());
        } catch (Exception e) {
            throw new DtLoaderException(String.format("SQL executed exception, %s", e.getMessage()), e);
        } finally {
            DBUtil.closeDBResources(null, null, DBUtil.clearAfterGetConnection(hiveSourceDTO, clearStatus));
        }
        return tableInfo;
    }

    private void getTable(Table tableInfo, HiveSourceDTO hiveSourceDTO, String tableName) {
        List<Map<String, Object>> result = executeQuery(hiveSourceDTO, SqlQueryDTO.builder().sql("desc formatted " + tableName).build(), ConnectionClearStatus.NORMAL.getValue());
        boolean isTableInfo = false;
        for (Map<String, Object> row : result) {
            String colName = MapUtils.getString(row, "col_name", "");
            String comment = MapUtils.getString(row, "comment", "");
            String dataTypeOrigin = MapUtils.getString(row, "data_type", "");
            if (StringUtils.isBlank(colName) || StringUtils.isBlank(dataTypeOrigin)) {
                if (StringUtils.isNotBlank(colName) && colName.contains("# Detailed Table Information")) {
                    isTableInfo = true;
                }
            }
            // 去空格处理
            String dataType = dataTypeOrigin.trim();
            if (!isTableInfo) {
                continue;
            }

            if (colName.contains("Location")) {
                tableInfo.setPath(dataType);
                continue;
            }

            if (colName.contains("Type")) {
                tableInfo.setExternalOrManaged(dataType);
                continue;
            }

            if (colName.contains("field.delim")) {
                // 列分隔符不进行 trim 操作
                tableInfo.setDelim(dataTypeOrigin);
                continue;
            }

            if (dataType.contains("field.delim")) {
                tableInfo.setDelim(MapUtils.getString(row, "comment", "\001").trim());
                continue;
            }

            if (colName.contains("Owner")) {
                tableInfo.setOwner(dataType);
                continue;
            }

            if (colName.contains("CreateTime") || colName.contains("CreatedTime")) {
                tableInfo.setCreatedTime(dataType);
                continue;
            }

            if (colName.contains("LastAccess")) {
                tableInfo.setLastAccess(dataType);
                continue;
            }

            if (colName.contains("CreatedBy")) {
                tableInfo.setCreatedBy(dataType);
                continue;
            }

            if (colName.contains("Database")) {
                tableInfo.setDb(dataType);
                continue;
            }

            if (StringUtils.containsIgnoreCase(dataType, "transactional")) {
                if (ReflectUtil.fieldExists(Table.class, "isTransTable") && StringUtils.containsIgnoreCase(comment, "true")) {
                    tableInfo.setIsTransTable(true);
                }
                continue;
            }

            if (StringUtils.containsIgnoreCase(colName, "Type")) {
                if (ReflectUtil.fieldExists(Table.class, "isView")) {
                    tableInfo.setIsView(StringUtils.containsIgnoreCase(dataType, "VIEW"));
                }
                continue;
            }

            if (tableInfo.getStoreType() == null && colName.contains("InputFormat")) {
                for (StoredType hiveStoredType : StoredType.values()) {
                    if (dataType.contains(hiveStoredType.getInputFormatClass())) {
                        tableInfo.setStoreType(hiveStoredType.getValue());
                        break;
                    }
                }
            }
        }
    }

    @Override
    protected String getCurrentDbSql() {
        return CURRENT_DB;
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
}
