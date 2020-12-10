package com.dtstack.dtcenter.common.loader.spark;

import com.alibaba.fastjson.JSONObject;
import com.dtstack.dtcenter.common.exception.DBErrorCode;
import com.dtstack.dtcenter.common.exception.DtCenterDefException;
import com.dtstack.dtcenter.common.hadoop.HdfsOperator;
import com.dtstack.dtcenter.common.loader.common.AbsRdbmsClient;
import com.dtstack.dtcenter.common.loader.common.ConnFactory;
import com.dtstack.dtcenter.loader.DtClassConsistent;
import com.dtstack.dtcenter.loader.IDownloader;
import com.dtstack.dtcenter.loader.dto.ColumnMetaDTO;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;
import com.dtstack.dtcenter.loader.dto.source.SparkSourceDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import com.dtstack.dtcenter.loader.utils.DBUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
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
import java.util.Properties;
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
 * @Description：Spark 连接
 */
@Slf4j
public class SparkClient extends AbsRdbmsClient {

    // 获取正在使用数据库
    private static final String CURRENT_DB = "select current_database()";

    @Override
    protected ConnFactory getConnFactory() {
        return new SparkConnFactory();
    }

    @Override
    protected DataSourceType getSourceType() {
        return DataSourceType.Spark;
    }

    // 测试连通性超时时间。单位：秒
    private final static int TEST_CONN_TIMEOUT = 30;

    @Override
    public List<String> getTableList(ISourceDTO iSource, SqlQueryDTO queryDTO) throws Exception {
        Integer clearStatus = beforeQuery(iSource, queryDTO, false);
        SparkSourceDTO sparkSourceDTO = (SparkSourceDTO) iSource;
        // 获取表信息需要通过show tables 语句
        String sql = "show tables";
        Statement statement = null;
        ResultSet rs = null;
        List<String> tableList = new ArrayList<>();
        try {
            statement = sparkSourceDTO.getConnection().createStatement();
            if (StringUtils.isNotEmpty(sparkSourceDTO.getSchema())) {
                statement.execute(String.format(DtClassConsistent.PublicConsistent.USE_DB, sparkSourceDTO.getSchema()));
            }
            rs = statement.executeQuery(sql);
            int columnSize = rs.getMetaData().getColumnCount();
            while (rs.next()) {
                tableList.add(rs.getString(columnSize == 1 ? 1 : 2));
            }
        } catch (Exception e) {
            throw new DtCenterDefException("获取表异常", e);
        } finally {
            DBUtil.closeDBResources(rs, statement, sparkSourceDTO.clearAfterGetConnection(clearStatus));
        }
        return tableList;
    }

    @Override
    public String getTableMetaComment(ISourceDTO iSource, SqlQueryDTO queryDTO) throws Exception {
        Integer clearStatus = beforeColumnQuery(iSource, queryDTO);
        SparkSourceDTO sparkSourceDTO = (SparkSourceDTO) iSource;

        Statement statement = null;
        ResultSet resultSet = null;
        try {
            statement = sparkSourceDTO.getConnection().createStatement();
            if (StringUtils.isNotEmpty(sparkSourceDTO.getSchema())) {
                statement.execute(String.format(DtClassConsistent.PublicConsistent.USE_DB, sparkSourceDTO.getSchema()));
            }
            resultSet = statement.executeQuery(String.format(DtClassConsistent.HadoopConfConsistent.DESCRIBE_EXTENDED
                    , queryDTO.getTableName()));
            while (resultSet.next()) {
                String columnName = resultSet.getString(1);
                if (StringUtils.isNotEmpty(columnName) && DtClassConsistent.HadoopConfConsistent.TABLE_INFORMATION.equalsIgnoreCase(columnName)) {
                    String string = resultSet.getString(2);
                    if (StringUtils.isNotEmpty(string) && string.contains(DtClassConsistent.HadoopConfConsistent.COMMENT)) {
                        String[] split = string.split(DtClassConsistent.HadoopConfConsistent.COMMENT);
                        if (split.length > 1) {
                            return split[1].split(DtClassConsistent.PublicConsistent.LINE_SEPARATOR)[0].replace(" ",
                                    "");
                        }
                    }
                }
            }
        } catch (Exception e) {
            throw new DtCenterDefException(String.format("获取表:%s 的信息时失败. 请联系 DBA 核查该库、表信息.",
                    queryDTO.getTableName()),
                    DBErrorCode.GET_COLUMN_INFO_FAILED, e);
        } finally {
            DBUtil.closeDBResources(resultSet, statement, sparkSourceDTO.clearAfterGetConnection(clearStatus));
        }
        return "";
    }

    @Override
    public List<ColumnMetaDTO> getColumnMetaData(ISourceDTO iSource, SqlQueryDTO queryDTO) throws Exception {
        Integer clearStatus = beforeColumnQuery(iSource, queryDTO);
        SparkSourceDTO sparkSourceDTO = (SparkSourceDTO) iSource;

        List<ColumnMetaDTO> columnMetaDTOS = new ArrayList<>();
        Statement stmt = null;
        ResultSet resultSet = null;

        try {
            stmt = sparkSourceDTO.getConnection().createStatement();
            if (StringUtils.isNotEmpty(sparkSourceDTO.getSchema())) {
                stmt.execute(String.format(DtClassConsistent.PublicConsistent.USE_DB, sparkSourceDTO.getSchema()));
            }
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
            throw new DtCenterDefException(String.format("获取表:%s 的字段的元信息时失败. 请联系 DBA 核查该库、表信息.",
                    queryDTO.getTableName()),
                    DBErrorCode.GET_COLUMN_INFO_FAILED, e);
        } finally {
            DBUtil.closeDBResources(resultSet, stmt, sparkSourceDTO.clearAfterGetConnection(clearStatus));
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
            log.error("测试连通性超时！", e);
            throw new DtLoaderException("测试连通性超时！", e);
        } catch (Exception e){
            log.error("测试连通性出错！", e);
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
        SparkSourceDTO sparkSourceDTO = (SparkSourceDTO) iSource;
        if (StringUtils.isBlank(sparkSourceDTO.getDefaultFS())) {
            return Boolean.TRUE;
        }

        Properties properties = combineHdfsConfig(sparkSourceDTO.getConfig(), sparkSourceDTO.getKerberosConfig());
        Configuration conf = new HdfsOperator.HadoopConf().setConf(sparkSourceDTO.getDefaultFS(), properties);
        //不在做重复认证 主要用于 HdfsOperator.checkConnection 中有一些数栈自己的逻辑
        conf.set("hadoop.security.authorization", "false");
        conf.set("dfs.namenode.kerberos.principal.pattern", "*");

        // 校验高可用配置
        return KerberosUtil.loginWithUGI(sparkSourceDTO.getKerberosConfig()).doAs(
                (PrivilegedAction<Boolean>) () -> HdfsOperator.checkConnection(conf)
        );
    }



    /**
     * 高可用配置
     *
     * @param hadoopConfig
     * @param confMap
     * @return
     */
    private Properties combineHdfsConfig(String hadoopConfig, Map<String, Object> confMap) {
        Properties properties = new Properties();
        if (StringUtils.isNotBlank(hadoopConfig)) {
            try {
                Map<String, Object> hadoopMap = JSONObject.parseObject(hadoopConfig);
                properties.putAll(hadoopMap);
            } catch (Exception e) {
                throw new DtCenterDefException("高可用配置格式错误", e);
            }
        }
        if (confMap != null) {
            for (String key : confMap.keySet()) {
                properties.setProperty(key, confMap.get(key).toString());
            }
        }
        return properties;
    }

    @Override
    public IDownloader getDownloader(ISourceDTO iSource, SqlQueryDTO queryDTO) throws Exception {
        SparkSourceDTO sparkSourceDTO = (SparkSourceDTO) iSource;
        List<Map<String, Object>> list = executeQuery(sparkSourceDTO, SqlQueryDTO.builder().sql("desc formatted " + queryDTO.getTableName()).build());
        // 获取表路径、字段分隔符、存储方式
        String tableLocation = null;
        String fieldDelimiter = "\001";
        String storageMode = null;
        for (Map<String, Object> map : list) {
            String colName = (String) map.get("col_name");
            Object dataType = map.get("data_type");
            if (colName.contains("Location")) {
                tableLocation = (String) dataType;
                continue;
            }

            if (colName.contains("InputFormat")) {
                storageMode = (String) dataType;
                continue;
            }

            if (colName.contains("field.delim")) {
                fieldDelimiter = (String) dataType;
                break;
            }

            if (Objects.nonNull(dataType) && ((String) dataType).contains("field.delim")) {
                fieldDelimiter = (String) map.get("comment");
                break;
            }
        }
        // 获取表字段和分区字段
        ArrayList<String> columnNames = new ArrayList<>();
        ArrayList<String> partitionColumns = new ArrayList<>();
        List<ColumnMetaDTO> columnMetaData = getColumnMetaData(sparkSourceDTO, queryDTO);
        for (ColumnMetaDTO columnMetaDTO:columnMetaData){
            if (columnMetaDTO.getPart()){
                partitionColumns.add(columnMetaDTO.getKey());
                continue;
            }
            columnNames.add(columnMetaDTO.getKey());
        }
        // 校验高可用配置
        Configuration conf = null;
        if (StringUtils.isEmpty(sparkSourceDTO.getConfig())){
            throw new DtLoaderException("hadoop配置信息不能为空");
        }
        if (StringUtils.isBlank(sparkSourceDTO.getDefaultFS()) || !sparkSourceDTO.getDefaultFS().matches(DtClassConsistent.HadoopConfConsistent.DEFAULT_FS_REGEX)) {
            throw new DtCenterDefException("defaultFS格式不正确");
        }
        Properties properties = combineHdfsConfig(sparkSourceDTO.getConfig(), sparkSourceDTO.getKerberosConfig());
        if (properties.size() > 0) {
            conf = new HdfsOperator.HadoopConf().setConf(sparkSourceDTO.getDefaultFS(), properties);
            //不在做重复认证
            conf.set("hadoop.security.authorization", "false");
            //必须添加
            conf.set("dfs.namenode.kerberos.principal.pattern", "*");
        }

        String finalStorageMode = storageMode;
        Configuration finalConf = conf;
        String finalTableLocation = tableLocation;
        String finalFieldDelimiter = fieldDelimiter;
        return KerberosUtil.loginWithUGI(sparkSourceDTO.getKerberosConfig()).doAs(
                (PrivilegedAction<IDownloader>) () -> {
                    try {
                        return createDownloader(finalStorageMode, finalConf, finalTableLocation, columnNames, finalFieldDelimiter, partitionColumns, queryDTO.getPartitionColumns(), sparkSourceDTO.getKerberosConfig());
                    } catch (Exception e) {
                        throw new DtCenterDefException("创建下载器异常", e);
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
     * @param filterPartitions
     * @param kerberosConfig
     * @return
     * @throws Exception
     */
    private @NotNull IDownloader createDownloader(String storageMode, Configuration conf, String tableLocation, ArrayList<String> columnNames, String fieldDelimiter, ArrayList<String> partitionColumns, Map<String, String> filterPartitions, Map<String, Object> kerberosConfig) throws Exception {
        // 根据存储格式创建对应的hiveDownloader
        if (StringUtils.isBlank(storageMode)) {
            throw new DtCenterDefException("不支持该存储类型的hive表读取");
        }

        if (storageMode.contains("Text")){
            SparkTextDownload hiveTextDownload = new SparkTextDownload(conf, tableLocation, columnNames, fieldDelimiter, partitionColumns, filterPartitions,kerberosConfig);
            hiveTextDownload.configure();
            return hiveTextDownload;
        }

        if (storageMode.contains("Orc")){
            SparkORCDownload hiveORCDownload = new SparkORCDownload(conf, tableLocation, columnNames, partitionColumns, kerberosConfig);
            hiveORCDownload.configure();
            return hiveORCDownload;
        }

        if (storageMode.contains("Parquet")){
            SparkParquetDownload hiveParquetDownload = new SparkParquetDownload(conf, tableLocation, columnNames, partitionColumns, filterPartitions, kerberosConfig);
            hiveParquetDownload.configure();
            return hiveParquetDownload;
        }

        throw new DtCenterDefException("不支持该存储类型的hive表读取");
    }

    /**
     * 处理hive分区信息和sql语句
     * @param sqlQueryDTO 查询条件
     * @return
     */
    @Override
    protected String dealSql(SqlQueryDTO sqlQueryDTO) {
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
        return "select * from " + sqlQueryDTO.getTableName()
                +partSql.toString() + " limit " + sqlQueryDTO.getPreviewNum();
    }

    @Override
    public List<ColumnMetaDTO> getPartitionColumn(ISourceDTO source, SqlQueryDTO queryDTO) throws Exception {
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
    protected String getCurrentDbSql() {
        return CURRENT_DB;
    }
}
