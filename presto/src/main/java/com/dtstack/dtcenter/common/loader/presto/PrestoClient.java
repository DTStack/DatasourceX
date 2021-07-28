package com.dtstack.dtcenter.common.loader.presto;

import com.dtstack.dtcenter.common.loader.common.exception.ErrorCode;
import com.dtstack.dtcenter.common.loader.common.utils.DBUtil;
import com.dtstack.dtcenter.common.loader.common.utils.ReflectUtil;
import com.dtstack.dtcenter.common.loader.rdbms.AbsRdbmsClient;
import com.dtstack.dtcenter.common.loader.rdbms.ConnFactory;
import com.dtstack.dtcenter.loader.dto.ColumnMetaDTO;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;
import com.dtstack.dtcenter.loader.dto.source.PrestoSourceDTO;
import com.dtstack.dtcenter.loader.dto.source.RdbmsSourceDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import com.google.common.collect.Maps;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * presto 数据源客户端，目前 catalog 信息必须要在url指定，schema信息可以不指定
 *
 * @author ：wangchuan
 * date：Created in 上午9:50 2021/3/23
 * company: www.dtstack.com
 */
@Slf4j
public class PrestoClient<T> extends AbsRdbmsClient<T> {

    // 模糊查询schema
    private static final String SHOW_DB_LIKE = "SHOW SCHEMAS LIKE '%s'";

    // 创建schema
    private static final String CREATE_SCHEMA = "CREATE SCHEMA IF NOT EXISTS %s";

    // 判断table是否在schema中 ，不过滤视图
    private static final String TABLE_IS_IN_SCHEMA = "SELECT table_name FROM information_schema.tables WHERE table_schema='%s' AND table_name = '%s'";

    // 获取presto 数据源列表
    private static final String SHOW_CATALOG_SQL = "SHOW CATALOGS";

    // 获取当前数据源的schema
    private static final String SHOW_SCHEMA_SQL = "SHOW SCHEMAS";

    // 获取当前数据源下的表
    private static final String SHOW_TABLE_SQL = "SHOW TABLES";

    // 获取指定schema下的表，包括视图
    private static final String SHOW_TABLE_AND_VIEW_BY_SCHEMA_SQL = "SELECT table_name FROM information_schema.tables WHERE table_schema = '%s' ";

    // 获取指定schema下的表，不包括视图
    private static final String SHOW_TABLE_BY_SCHEMA_SQL = "SELECT table_name FROM information_schema.tables WHERE table_schema = '%s' AND table_type = 'BASE TABLE' ";

    // 获取所有表名，包括视图，表名前拼接schema，并对schema和tableName进行增加双引号处理
    private static final String ALL_TABLE_AND_VIEW_SQL = "SELECT '\"'||table_schema||'\".\"'||table_name||'\"' AS schema_table FROM information_schema.tables order by schema_table ";

    // 获取所有表名，不包括视图，表名前拼接schema，并对schema和tableName进行增加双引号处理
    private static final String ALL_TABLE_SQL = "SELECT '\"'||table_schema||'\".\"'||table_name||'\"' AS schema_table FROM information_schema.tables WHERE table_type = 'BASE TABLE'  order by schema_table ";

    // 获取建表语句
    private static final String SHOW_CREATE_TABLE_SQL = "SHOW CREATE TABLE %s";

    // 表注释信息
    private static final String SHOW_TABLE_COLUMN_COMMENT = "DESCRIBE %s";

    // DESCRIBE 查出表字段的key
    private static final String DESCRIBE_COLUMN = "Column";

    // DESCRIBE 查出表注释的key
    private static final String DESCRIBE_COMMENT = "Comment";

    // 未设置schema读取表报错信息
    private static final String SCHEMA_MUST_BE_SET = "Schema must be specified";

    @Override
    protected ConnFactory getConnFactory() {
        return new PrestoConnFactory();
    }

    @Override
    protected DataSourceType getSourceType() {
        return DataSourceType.Presto;
    }

    @Override
    public List<String> getTableList(ISourceDTO sourceDTO, SqlQueryDTO queryDTO) {
        try {
            Integer fetchSize = ReflectUtil.fieldExists(SqlQueryDTO.class, "fetchSize") ? queryDTO.getFetchSize() : null;
            return queryWithSingleColumn(sourceDTO, fetchSize, SHOW_TABLE_SQL, 1,"get table exception according to schema...");
        } catch (Exception e) {
            // 如果url 中没有指定到 schema，则获取当前catalog下的所有表，并拼接形式如 "schema"."table"
            if (e.getMessage().contains(SCHEMA_MUST_BE_SET)) {
                return getTableListBySchema(sourceDTO, queryDTO);
            }
            throw new DtLoaderException(e.getMessage(), e);
        }
    }

    @Override
    public String getTableMetaComment(ISourceDTO sourceDTO, SqlQueryDTO queryDTO) {
        // presto不支持获取表注释
        throw new DtLoaderException(ErrorCode.NOT_SUPPORT.getDesc());
    }

    @Override
    protected Map<String, String> getColumnComments(RdbmsSourceDTO sourceDTO, SqlQueryDTO queryDTO) {
        // 查询某表字段注释的sql
        String queryColumnCommentSql = String.format(SHOW_TABLE_COLUMN_COMMENT, transferSchemaAndTableName(sourceDTO, queryDTO));
        log.info("The SQL executed by method getColumnComments is:{}", queryColumnCommentSql);
        List<Map<String, Object>> result = executeQuery(sourceDTO, SqlQueryDTO.builder().sql(queryColumnCommentSql).build());
        Map<String, String> columnComments = Maps.newHashMap();
        if (CollectionUtils.isNotEmpty(result)) {
            result.stream().filter(MapUtils::isNotEmpty).forEach(row -> {
                String column = MapUtils.getString(row, DESCRIBE_COLUMN);
                if (StringUtils.isNotBlank(column)) {
                    columnComments.putIfAbsent(column, MapUtils.getString(row, DESCRIBE_COMMENT));
                }
            });
        }
        return columnComments;
    }

    @Override
    public Boolean isDatabaseExists(ISourceDTO source, String schema) {
        if (StringUtils.isBlank(schema)) {
            throw new DtLoaderException("database schema not null");
        }
        return CollectionUtils.isNotEmpty(executeQuery(source, SqlQueryDTO.builder().sql(String.format(SHOW_DB_LIKE, schema)).build()));
    }

    @Override
    public Boolean isTableExistsInDatabase(ISourceDTO source, String tableName, String dbName) {
        if (StringUtils.isBlank(dbName)) {
            throw new DtLoaderException("database name is not empty");
        }
        return CollectionUtils.isNotEmpty(executeQuery(source, SqlQueryDTO.builder().sql(String.format(TABLE_IS_IN_SCHEMA, dbName, tableName)).build()));
    }

    /**
     * 获取指定schema下的表，如果没有填schema，则获取全部schema的表
     *
     * @param sourceDTO 数据源信息
     * @param queryDTO  查询条件
     * @return 查询sql
     */
    @Override
    protected String getTableBySchemaSql(ISourceDTO sourceDTO, SqlQueryDTO queryDTO) {
        PrestoSourceDTO prestoSourceDTO = (PrestoSourceDTO) sourceDTO;
        // schema 先从queryDTO中获取
        String schema = StringUtils.isBlank(queryDTO.getSchema()) ? prestoSourceDTO.getSchema() : queryDTO.getSchema();
        // 包括视图
        if (BooleanUtils.isTrue(queryDTO.getView())) {
            // schema不传了则获取全部schema下的表
            if (StringUtils.isBlank(schema)) {
                return ALL_TABLE_AND_VIEW_SQL;
            } else {
                return String.format(SHOW_TABLE_AND_VIEW_BY_SCHEMA_SQL, schema);
            }
        } else {
            if (StringUtils.isBlank(schema)) {
                return ALL_TABLE_SQL;
            } else {
                return String.format(SHOW_TABLE_BY_SCHEMA_SQL, schema);
            }
        }
    }

    @Override
    public List<ColumnMetaDTO> getColumnMetaData(ISourceDTO iSource, SqlQueryDTO queryDTO) {
        Integer clearStatus = beforeColumnQuery(iSource, queryDTO);
        PrestoSourceDTO prestoSourceDTO = (PrestoSourceDTO) iSource;
        // schema 先从queryDTO中获取
        String schema = StringUtils.isBlank(queryDTO.getSchema()) ? prestoSourceDTO.getSchema() : queryDTO.getSchema();
        try {
            return getColumnMetaData(prestoSourceDTO.getConnection(), schema, queryDTO.getTableName());
        } finally {
            DBUtil.closeDBResources(null, null, DBUtil.clearAfterGetConnection(prestoSourceDTO, clearStatus));
        }
    }

    private List<ColumnMetaDTO> getColumnMetaData(Connection conn, String schema, String tableName) {
        List<ColumnMetaDTO> columnList = new ArrayList<>();
        Statement stmt = null;
        ResultSet resultSet = null;
        try {
            stmt = conn.createStatement();
            resultSet = stmt.executeQuery("DESCRIBE " + transferSchemaAndTableName(schema, tableName));
            while (resultSet.next()) {
                String colName = resultSet.getString("Column");
                String dataType = resultSet.getString("Type");
                String extra = resultSet.getString("Extra");
                String comment = resultSet.getString("Comment");
                ColumnMetaDTO metaDTO = new ColumnMetaDTO();
                metaDTO.setKey(colName);
                metaDTO.setType(dataType.trim());
                metaDTO.setComment(comment);
                if (StringUtils.isNotBlank(extra) && extra.contains("partition key")) {
                    metaDTO.setPart(true);
                }
                columnList.add(metaDTO);
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
    public String getCreateTableSql(ISourceDTO sourceDTO, SqlQueryDTO queryDTO) {
        PrestoSourceDTO prestoSourceDTO = (PrestoSourceDTO) sourceDTO;
        // 对表名进行转换，schema如果传则不可以在表名中出现schema信息
        String tableName = transferSchemaAndTableName(prestoSourceDTO, queryDTO);
        List<String> result = queryWithSingleColumn(sourceDTO, null, String.format(SHOW_CREATE_TABLE_SQL, tableName), 1, "failed to get table create sql...");
        if (CollectionUtils.isEmpty(result)) {
            throw new DtLoaderException("failed to get table create sql...");
        }
        return result.get(0);
    }

    @Override
    protected String getShowDbSql() {
        return SHOW_SCHEMA_SQL;
    }

    @Override
    protected String getCatalogSql() {
        return SHOW_CATALOG_SQL;
    }

    /**
     * 获取创建schema的语句，不支持注释
     *
     * @param schema  schema
     * @param comment 注释
     * @return sql语句
     */
    @Override
    protected String getCreateDatabaseSql(String schema, String comment) {
        return String.format(CREATE_SCHEMA, schema);
    }

    /**
     * 处理 presto schema和tableName，适配schema和tableName中有.的情况
     *
     * @param schema    schema名
     * @param tableName 表名
     * @return 处理后的数据
     */
    @Override
    protected String transferSchemaAndTableName(String schema, String tableName) {
        if (StringUtils.isBlank(schema)) {
            return tableName;
        }
        return String.format("%s.%s", schema, tableName);
    }
}
