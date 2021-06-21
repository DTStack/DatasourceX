package com.dtstack.dtcenter.common.loader.impala;

import com.alibaba.fastjson.JSONObject;
import com.dtstack.dtcenter.common.loader.common.DtClassConsistent;
import com.dtstack.dtcenter.common.loader.common.enums.StoredType;
import com.dtstack.dtcenter.common.loader.common.utils.DBUtil;
import com.dtstack.dtcenter.common.loader.rdbms.AbsRdbmsClient;
import com.dtstack.dtcenter.common.loader.rdbms.ConnFactory;
import com.dtstack.dtcenter.loader.IDownloader;
import com.dtstack.dtcenter.loader.dto.ColumnMetaDTO;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.Table;
import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;
import com.dtstack.dtcenter.loader.dto.source.ImpalaSourceDTO;
import com.dtstack.dtcenter.loader.enums.ConnectionClearStatus;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 20:16 2020/1/7
 * @Description：Impala 连接
 */
public class ImpalaClient extends AbsRdbmsClient {

    // 获取正在使用数据库
    private static final String CURRENT_DB = "select current_database()";

    private static final String SHOW_TABLE_SQL = "show tables %s";

    // 根据schema选表表名模糊查询
    private static final String SEARCH_SQL = " LIKE '%s' ";

    // 获取当前版本号
    private static final String SHOW_VERSION = "select version()";

    @Override
    protected ConnFactory getConnFactory() {
        return new ImpalaConnFactory();
    }

    @Override
    protected DataSourceType getSourceType() {
        return DataSourceType.IMPALA;
    }

    @Override
    public List<String> getTableList(ISourceDTO iSource, SqlQueryDTO queryDTO) {
        Integer clearStatus = beforeQuery(iSource, queryDTO, false);
        ImpalaSourceDTO impalaSourceDTO = (ImpalaSourceDTO) iSource;
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
            statement = impalaSourceDTO.getConnection().createStatement();
            if (Objects.nonNull(queryDTO) && Objects.nonNull(queryDTO.getLimit())) {
                // 设置最大条数
                statement.setMaxRows(queryDTO.getLimit());
            }
            rs = statement.executeQuery(sql);
            int columnSize = rs.getMetaData().getColumnCount();
            while (rs.next()) {
                tableList.add(rs.getString(columnSize == 1 ? 1 : 2));
            }
        } catch (Exception e) {
            throw new DtLoaderException(String.format("get table exception：%s", e.getMessage()), e);
        } finally {
            DBUtil.closeDBResources(rs, statement, DBUtil.clearAfterGetConnection(impalaSourceDTO, clearStatus));
        }
        return tableList;
    }

    @Override
    public List<String> getTableListBySchema(ISourceDTO source, SqlQueryDTO queryDTO) {
        return getTableList(source, queryDTO);
    }

    @Override
    public List<ColumnMetaDTO> getColumnMetaData(ISourceDTO iSource, SqlQueryDTO queryDTO) {
        Integer clearStatus = beforeColumnQuery(iSource, queryDTO);
        ImpalaSourceDTO impalaSourceDTO = (ImpalaSourceDTO) iSource;
        // schema 先从queryDTO中获取
        String schema = StringUtils.isBlank(queryDTO.getSchema()) ? impalaSourceDTO.getSchema() : queryDTO.getSchema();
        try {
            return getColumnMetaData(impalaSourceDTO.getConnection(), schema, queryDTO.getTableName(), queryDTO.getFilterPartitionColumns());
        } finally {
            DBUtil.closeDBResources(null, null, DBUtil.clearAfterGetConnection(impalaSourceDTO, clearStatus));
        }
    }

    private List<ColumnMetaDTO> getColumnMetaData(Connection conn, String schema, String tableName, Boolean filterPartitionColumns) {
        List<ColumnMetaDTO> columnList = new ArrayList<>();
        Statement stmt = null;
        ResultSet resultSet = null;
        try {
            LinkedHashMap<String, JSONObject> colNameMap = new LinkedHashMap<>();
            stmt = conn.createStatement();
            //首先判断是否是kudu表 是kudu表直接用主键代替 isPart
            resultSet = stmt.executeQuery("DESCRIBE " + transferSchemaAndTableName(schema, tableName));
            int columnCnt = resultSet.getMetaData().getColumnCount();

            // kudu表
            if (columnCnt > 3) {
                while (resultSet.next()) {
                    columnList.add(dealResult(resultSet,
                            resultSet.getString(DtClassConsistent.PublicConsistent.PRIMARY_KEY)));
                }
                return columnList;
            }

            //hive表 继续获取分区字段 先关闭之前的 rs
            resultSet.close();
            resultSet = stmt.executeQuery("DESCRIBE formatted " + transferSchemaAndTableName(schema, tableName));
            while (resultSet.next()) {
                String colName = resultSet.getString(DtClassConsistent.PublicConsistent.NAME).trim();

                if (StringUtils.isEmpty(colName)) {
                    continue;
                }
                if (colName.startsWith("#") && colName.contains(DtClassConsistent.PublicConsistent.COL_NAME)) {
                    continue;

                }
                if (colName.startsWith("#") || colName.contains("Partition Information")) {
                    break;
                }

                if (StringUtils.isNotBlank(colName)) {
                    columnList.add(dealResult(resultSet, Boolean.FALSE));
                }
            }

            while (resultSet.next() && !filterPartitionColumns) {
                String colName = resultSet.getString(DtClassConsistent.PublicConsistent.NAME);
                if (StringUtils.isBlank(colName)) {
                    continue;
                }
                if (colName.startsWith("#") && colName.contains(DtClassConsistent.PublicConsistent.COL_NAME)) {
                    continue;
                }
                if (colName.contains("Detailed Table Information") || colName.contains("Database:")) {
                    break;
                }

                columnList.add(dealResult(resultSet, Boolean.TRUE));
            }

        } catch (SQLException e) {
            throw new DtLoaderException(String.format("Failed to get meta information for the fields of table :%s. Please contact the DBA to check the database table information.",
                    transferSchemaAndTableName(schema, tableName)), e);
        } finally {
            DBUtil.closeDBResources(resultSet, stmt, null);
        }

        return columnList;
    }

    @Override
    public String getTableMetaComment(ISourceDTO iSource, SqlQueryDTO queryDTO) {
        Integer clearStatus = beforeColumnQuery(iSource, queryDTO);
        ImpalaSourceDTO impalaSourceDTO = (ImpalaSourceDTO) iSource;
        // schema 先从queryDTO中获取
        String schema = StringUtils.isBlank(queryDTO.getSchema()) ? impalaSourceDTO.getSchema() : queryDTO.getSchema();
        try {
            return getTableMetaComment(impalaSourceDTO.getConnection(), schema, queryDTO.getTableName());
        } finally {
            DBUtil.closeDBResources(null, null, DBUtil.clearAfterGetConnection(impalaSourceDTO, clearStatus));
        }
    }

    private String getTableMetaComment(Connection conn, String schema, String tableName) {

        Statement statement = null;
        ResultSet resultSet = null;
        try {
            statement = conn.createStatement();
            resultSet = statement.executeQuery(String.format(DtClassConsistent.HadoopConfConsistent.DESCRIBE_EXTENDED
                    , transferSchemaAndTableName(schema, tableName)));
            while (resultSet.next()) {
                String columnType = resultSet.getString(2);
                if (StringUtils.isNotBlank(columnType) && columnType.contains("comment")) {
                    return StringUtils.isBlank(resultSet.getString(3)) ? "" : resultSet.getString(3).trim();
                }
            }
        } catch (Exception e) {
            throw new DtLoaderException(String.format("get table: %s's information error. Please contact the DBA to check the database、table information.",
                    transferSchemaAndTableName(schema, tableName)), e);
        } finally {
            DBUtil.closeDBResources(resultSet, statement, null);
        }
        return "";
    }

    private static ColumnMetaDTO dealResult(ResultSet resultSet, Object part) throws SQLException {
        ColumnMetaDTO metaDTO = new ColumnMetaDTO();
        metaDTO.setKey(resultSet.getString(DtClassConsistent.PublicConsistent.NAME).trim());
        metaDTO.setType(resultSet.getString(DtClassConsistent.PublicConsistent.TYPE).trim());
        metaDTO.setComment(resultSet.getString(DtClassConsistent.PublicConsistent.COMMENT));
        metaDTO.setPart(Boolean.TRUE.equals(part));
        return metaDTO;
    }

    @Override
    public List<String> getAllDatabases(ISourceDTO source, SqlQueryDTO queryDTO) {
        List<String> databases = super.getAllDatabases(source, queryDTO);
        databases.remove(0);
        return databases;
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
        Integer clearStatus = beforeQuery(source, queryDTO, false);
        ImpalaSourceDTO impalaSourceDTO = (ImpalaSourceDTO) source;

        Table tableInfo = new Table();
        // schema 先从queryDTO中获取
        String schema = StringUtils.isBlank(queryDTO.getSchema()) ? impalaSourceDTO.getSchema() : queryDTO.getSchema();
        try {
            tableInfo.setName(queryDTO.getTableName());
            // 获取表注释
            tableInfo.setComment(getTableMetaComment(impalaSourceDTO.getConnection(), schema, queryDTO.getTableName()));
            // 处理字段信息
            tableInfo.setColumns(getColumnMetaData(impalaSourceDTO.getConnection(), schema, queryDTO.getTableName(), queryDTO.getFilterPartitionColumns()));
            // 获取表结构信息
            getTable(tableInfo, impalaSourceDTO, schema, queryDTO.getTableName());
        } catch (Exception e) {
            throw new DtLoaderException(String.format("SQL executed exception, %s", e.getMessage()), e);
        } finally {
            DBUtil.closeDBResources(null, null, DBUtil.clearAfterGetConnection(impalaSourceDTO, clearStatus));
        }
        return tableInfo;

    }

    private void getTable(Table tableInfo, ImpalaSourceDTO impalaSourceDTO, String schema, String tableName) {
        List<Map<String, Object>> result = executeQuery(impalaSourceDTO, SqlQueryDTO.builder().sql("DESCRIBE formatted " + transferSchemaAndTableName(schema, tableName)).build(), ConnectionClearStatus.NORMAL.getValue());
        boolean isTableInfo = false;
        for (Map<String, Object> row : result) {
            String colName = MapUtils.getString(row, "name", "");
            String dataType = MapUtils.getString(row, "type", "");
            if (StringUtils.isBlank(colName) || StringUtils.isBlank(dataType)) {
                if (StringUtils.isNotBlank(colName) && colName.contains("# Detailed Table Information")) {
                    isTableInfo = true;
                }
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

            if (dataType.contains("field.delim")) {
                tableInfo.setDelim(MapUtils.getString(row, "comment", "").trim());
                continue;
            }

            if (colName.contains("Owner")) {
                tableInfo.setOwner(dataType);
                continue;
            }

            if (colName.contains("CreateTime")) {
                tableInfo.setCreatedTime(dataType);
                continue;
            }

            if (colName.contains("LastAccessTime")) {
                tableInfo.setLastAccess(dataType);
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
    }

    @Override
    protected String getCurrentDbSql() {
        return CURRENT_DB;
    }

    /**
     * 处理 schema和tableName
     *
     * @param schema
     * @param tableName
     * @return
     */
    @Override
    protected String transferSchemaAndTableName(String schema, String tableName) {
        if (StringUtils.isBlank(schema)) {
            return tableName;
        }
        //如果tableName带 . 如 database.table 直接返回
        if(StringUtils.isNotEmpty(tableName) && tableName.contains(".")){
            return tableName;
        }
        return String.format("%s.%s", schema, tableName);
    }

    @Override
    public IDownloader getDownloader(ISourceDTO source, SqlQueryDTO queryDTO) throws Exception {
        ImpalaDownload impalaDownload = new ImpalaDownload(getCon(source), queryDTO.getSql());
        impalaDownload.configure();
        return impalaDownload;
    }

    @Override
    protected String addPercentSign(String str) {
        return "*" + str + "*";
    }

    @Override
    protected String getVersionSql() {
        return SHOW_VERSION;
    }
}
