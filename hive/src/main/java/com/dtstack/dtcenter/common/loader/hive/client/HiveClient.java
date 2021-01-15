package com.dtstack.dtcenter.common.loader.hive.client;

import com.dtstack.dtcenter.common.loader.common.DtClassConsistent;
import com.dtstack.dtcenter.common.loader.common.enums.StoredType;
import com.dtstack.dtcenter.common.loader.common.utils.DBUtil;
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
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.jetbrains.annotations.NotNull;

import java.security.PrivilegedAction;
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
            throw new DtLoaderException("获取表异常", e);
        } finally {
            DBUtil.closeDBResources(rs, statement, hiveSourceDTO.clearAfterGetConnection(clearStatus));
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

        Statement statement = null;
        ResultSet resultSet = null;
        try {
            statement = hiveSourceDTO.getConnection().createStatement();
            resultSet = statement.executeQuery(String.format(DtClassConsistent.HadoopConfConsistent.DESCRIBE_EXTENDED
                    , queryDTO.getTableName()));
            while (resultSet.next()) {
                String columnName = resultSet.getString(1);
                if (StringUtils.isNotEmpty(columnName) && DtClassConsistent.HadoopConfConsistent.TABLE_INFORMATION.equalsIgnoreCase(columnName)) {
                    String string = resultSet.getString(2);
                    if (StringUtils.isNotEmpty(string) && string.contains(DtClassConsistent.HadoopConfConsistent.COMMENT)) {
                        String[] split = string.split(DtClassConsistent.HadoopConfConsistent.COMMENT);
                        if (split.length > 1) {
                            return split[1].split(DtClassConsistent.PublicConsistent.LINE_SEPARATOR)[0].trim();
                        }
                    }
                }
            }
        } catch (Exception e) {
            throw new DtLoaderException(String.format("获取表:%s 的信息时失败. 请联系 DBA 核查该库、表信息.",
                    queryDTO.getTableName()), e);
        } finally {
            DBUtil.closeDBResources(resultSet, statement, hiveSourceDTO.clearAfterGetConnection(clearStatus));
        }
        return "";
    }

    @Override
    public List<ColumnMetaDTO> getColumnMetaData(ISourceDTO iSource, SqlQueryDTO queryDTO) {
        Integer clearStatus = beforeColumnQuery(iSource, queryDTO);
        HiveSourceDTO hiveSourceDTO = (HiveSourceDTO) iSource;

        List<ColumnMetaDTO> columnMetaDTOS = new ArrayList<>();
        Statement stmt = null;
        ResultSet resultSet = null;

        try {
            stmt = hiveSourceDTO.getConnection().createStatement();
            resultSet = stmt.executeQuery("desc extended " + queryDTO.getTableName());
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
            resultSet = stmt.executeQuery("desc extended " + queryDTO.getTableName());
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

            return columnMetaDTOS.stream().filter(column -> !queryDTO.getFilterPartitionColumns() || !column.getPart()).collect(Collectors.toList());
        } catch (SQLException e) {
            throw new DtLoaderException(String.format("获取表:%s 的字段的元信息时失败. 请联系 DBA 核查该库、表信息.",
                    queryDTO.getTableName()), e);
        } finally {
            DBUtil.closeDBResources(resultSet, stmt, hiveSourceDTO.clearAfterGetConnection(clearStatus));
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
            throw new DtLoaderException("测试连通性超时！", e);
        } catch (Exception e){
            throw new DtLoaderException("测试连通性出错！", e);
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
        HiveSourceDTO hiveSourceDTO= (HiveSourceDTO) iSource;
        if (StringUtils.isBlank(hiveSourceDTO.getDefaultFS())) {
            return Boolean.TRUE;
        }

        return HdfsOperator.checkConnection(hiveSourceDTO.getDefaultFS(), hiveSourceDTO.getConfig(), hiveSourceDTO.getKerberosConfig());
    }

    @Override
    public IDownloader getDownloader(ISourceDTO iSource, SqlQueryDTO queryDTO) {
        HiveSourceDTO hiveSourceDTO= (HiveSourceDTO) iSource;
        List<Map<String, Object>> list = executeQuery(hiveSourceDTO, SqlQueryDTO.builder().sql("desc formatted " + queryDTO.getTableName()).build());
        // 获取表路径、字段分隔符、存储方式
        String tableLocation = null;
        String fieldDelimiter = "\001";
        String storageMode = null;
        for (Map<String, Object> map : list) {
            String colName = MapUtils.getString(map, "col_name");
            String dataType = MapUtils.getString(map, "data_type");
            if (colName.contains("Location")) {
                tableLocation = dataType;
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
        // 获取表字段和分区字段
        ArrayList<String> columnNames = new ArrayList<>();
        ArrayList<String> partitionColumns = new ArrayList<>();
        List<ColumnMetaDTO> columnMetaData = getColumnMetaData(hiveSourceDTO, queryDTO);
        for (ColumnMetaDTO columnMetaDTO:columnMetaData){
            if (columnMetaDTO.getPart()){
                partitionColumns.add(columnMetaDTO.getKey());
                continue;
            }
            columnNames.add(columnMetaDTO.getKey());
        }
        // 校验高可用配置
        if (StringUtils.isBlank(hiveSourceDTO.getDefaultFS()) || !hiveSourceDTO.getDefaultFS().matches(DtClassConsistent.HadoopConfConsistent.DEFAULT_FS_REGEX)) {
            throw new DtLoaderException("defaultFS格式不正确");
        }
        Configuration conf = HadoopConfUtil.getHdfsConf(hiveSourceDTO.getDefaultFS(), hiveSourceDTO.getConfig(), hiveSourceDTO.getKerberosConfig());

        String finalStorageMode = storageMode;
        Configuration finalConf = conf;
        String finalTableLocation = tableLocation;
        String finalFieldDelimiter = fieldDelimiter;
        return KerberosLoginUtil.loginWithUGI(hiveSourceDTO.getKerberosConfig()).doAs(
                (PrivilegedAction<IDownloader>) () -> {
                    try {
                        return createDownloader(finalStorageMode, finalConf, finalTableLocation, columnNames, finalFieldDelimiter, partitionColumns, queryDTO.getPartitionColumns(), hiveSourceDTO.getKerberosConfig());
                    } catch (Exception e) {
                        throw new DtLoaderException("创建下载器异常", e);
                    }
                }
        );
    }

    /**
     * 根据存储格式创建对应的hiveDownloader
     * @param storageMode
     * @param conf
     * @param tableLocation
     * @param columnNames
     * @param fieldDelimiter
     * @param partitionColumns
     * @return
     * @throws Exception
     */
    private @NotNull IDownloader createDownloader(String storageMode, Configuration conf, String tableLocation, ArrayList<String> columnNames, String fieldDelimiter, ArrayList<String> partitionColumns, Map<String, String> filterPartitions, Map<String, Object> kerberosConfig) throws Exception {
        // 根据存储格式创建对应的hiveDownloader
        if (StringUtils.isBlank(storageMode)) {
            throw new DtLoaderException("不支持该存储类型的hive表读取");
        }

        if (storageMode.contains("Text")){
            HiveTextDownload hiveTextDownload = new HiveTextDownload(conf, tableLocation, columnNames, fieldDelimiter, partitionColumns, filterPartitions, kerberosConfig);
            hiveTextDownload.configure();
            return hiveTextDownload;
        }

        if (storageMode.contains("Orc")){
            HiveORCDownload hiveORCDownload = new HiveORCDownload(conf, tableLocation, columnNames, partitionColumns, kerberosConfig);
            hiveORCDownload.configure();
            return hiveORCDownload;
        }

        if (storageMode.contains("Parquet")){
            HiveParquetDownload hiveParquetDownload = new HiveParquetDownload(conf, tableLocation, columnNames, partitionColumns, filterPartitions, kerberosConfig);
            hiveParquetDownload.configure();
            return hiveParquetDownload;
        }

        throw new DtLoaderException("不支持该存储类型的hive表读取");
    }

    /**
     * 处理hive分区信息和sql语句
     * @param sqlQueryDTO 查询条件
     * @return
     */
    @Override
    protected String dealSql(ISourceDTO iSourceDTO, SqlQueryDTO sqlQueryDTO) {
        Map<String, String> partitions = sqlQueryDTO.getPartitionColumns();
        StringBuilder partSql = new StringBuilder();
        //拼接分区信息
        if (MapUtils.isNotEmpty(partitions)){
            boolean check = true;
            partSql.append(" where ");
            Set<String> set = partitions.keySet();
            for (String column:set){
                if (check){
                    partSql.append(column+"=").append(partitions.get(column));
                    check = false;
                }else {
                    partSql.append(" and ").append(column+"=").append(partitions.get(column));
                }
            }
        }

        return "select * from " + sqlQueryDTO.getTableName() + partSql.toString();
    }

    @Override
    public List<ColumnMetaDTO> getPartitionColumn(ISourceDTO source, SqlQueryDTO queryDTO) {
        List<ColumnMetaDTO> columnMetaDTOS = getColumnMetaData(source,queryDTO);
        List<ColumnMetaDTO> partitionColumnMeta = new ArrayList<>();
        columnMetaDTOS.forEach(columnMetaDTO -> {
            if(columnMetaDTO.getPart()){
                partitionColumnMeta.add(columnMetaDTO);
            }
        });
        return partitionColumnMeta;
    }

    @Override
    public Table getTable(ISourceDTO source, SqlQueryDTO queryDTO) {
        Table tableInfo = new Table();
        tableInfo.setName(queryDTO.getTableName());
        tableInfo.setComment(getTableMetaComment(source, queryDTO));
        // 处理字段信息
        tableInfo.setColumns(getColumnMetaData(source, queryDTO));

        List<Map<String, Object>> result = executeQuery(source, SqlQueryDTO.builder().sql("desc formatted " + queryDTO.getTableName()).build());
        boolean isTableInfo = false;
        for (Map<String, Object> row : result) {
            String colName = MapUtils.getString(row, "col_name");
            String dataType = MapUtils.getString(row, "data_type");
            if (StringUtils.isBlank(colName) || StringUtils.isBlank(dataType)) {
                if (StringUtils.isNotBlank(colName) && colName.contains("# Detailed Table Information")) {
                    isTableInfo = true;
                }
                continue;
            }
            // 去空格处理
            dataType = dataType.trim();
            if (!isTableInfo) {
                continue;
            }

            if (colName.contains("Location:")) {
                tableInfo.setPath(dataType);
                continue;
            }

            if (colName.contains("Table Type:")) {
                tableInfo.setExternalOrManaged(dataType);
                continue;
            }

            if (colName.contains("field.delim")) {
                tableInfo.setDelim(dataType);
                continue;
            }

            if (colName.contains("Database")) {
                tableInfo.setDb(dataType);
                continue;
            }

            if (tableInfo.getStoreType() == null && colName.contains("InputFormat:")) {
                for (StoredType hiveStoredType : StoredType.values()) {
                    if (dataType.contains(hiveStoredType.getInputFormatClass())) {
                        tableInfo.setStoreType(hiveStoredType.getValue());
                        break;
                    }
                }
            }
        }
        return tableInfo;
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
            throw new DtLoaderException("数据库名称不能为空！");
        }
        return CollectionUtils.isNotEmpty(executeQuery(source, SqlQueryDTO.builder().sql(String.format(SHOW_DB_LIKE, dbName)).build()));
    }

    @Override
    public Boolean isTableExistsInDatabase(ISourceDTO source, String tableName, String dbName) {
        if (StringUtils.isBlank(dbName)) {
            throw new DtLoaderException("数据库名称不能为空！");
        }
        return CollectionUtils.isNotEmpty(executeQuery(source, SqlQueryDTO.builder().sql(String.format(TABLE_BY_SCHEMA_LIKE, dbName, tableName)).build()));
    }
}
