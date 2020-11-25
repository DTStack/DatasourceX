package com.dtstack.dtcenter.common.loader.oracle;

import com.dtstack.dtcenter.common.loader.common.DtClassConsistent;
import com.dtstack.dtcenter.common.loader.common.utils.CollectionUtil;
import com.dtstack.dtcenter.common.loader.common.utils.DBUtil;
import com.dtstack.dtcenter.common.loader.rdbms.AbsRdbmsClient;
import com.dtstack.dtcenter.common.loader.rdbms.ConnFactory;
import com.dtstack.dtcenter.loader.IDownloader;
import com.dtstack.dtcenter.loader.dto.ColumnMetaDTO;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;
import com.dtstack.dtcenter.loader.dto.source.OracleSourceDTO;
import com.dtstack.dtcenter.loader.dto.source.RdbmsSourceDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import lombok.extern.slf4j.Slf4j;
import oracle.jdbc.OracleResultSetMetaData;
import org.apache.commons.lang3.StringUtils;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 12:00 2020/1/6
 * @Description：Oracle 客户端
 */
@Slf4j
public class OracleClient extends AbsRdbmsClient {
    private static final String ORACLE_ALL_TABLES_SQL = "SELECT TABLE_NAME FROM USER_TABLES UNION SELECT '\"'||GRANTOR||'\"'||'.'||'\"'||TABLE_NAME||'\"' FROM ALL_TAB_PRIVS WHERE grantee = (SELECT USERNAME FROM user_users WHERE ROWNUM = 1) and table_schema != 'SYS' ";
    private static final String ORACLE_WITH_VIEWS_SQL = "UNION SELECT VIEW_NAME FROM USER_VIEWS ";

    private static String ORACLE_NUMBER_TYPE = "NUMBER";
    private static String ORACLE_NUMBER_FORMAT = "NUMBER(%d,%d)";
    private static String ORACLE_COLUMN_NAME = "COLUMN_NAME";
    private static String ORACLE_COLUMN_COMMENT = "COMMENTS";
    private static final String DONT_EXIST = "doesn't exist";

    private static final String DATABASE_QUERY = "select USERNAME from ALL_USERS order by USERNAME";

    private static final String TABLE_CREATE_SQL = "select dbms_metadata.get_ddl('TABLE','%s','%s') from dual";

    // 获取oracle默认使用的schema
    private static final String CURRENT_DB = "select sys_context('USERENV', 'CURRENT_SCHEMA') as schema_name from dual";

    /* -------------------------------------获取表操作相关sql------------------------------------- */
    // oracle获取指定schema下的表
    private static final String SHOW_TABLE_BY_SCHEMA_SQL = " SELECT TABLE_NAME FROM ALL_TABLES WHERE OWNER = '%s' %s ";
    // oracle获取指定schema下的视图
    private static final String SHOW_VIEW_BY_SCHEMA_SQL = " UNION SELECT VIEW_NAME FROM ALL_VIEWS WHERE OWNER = '%s' %s ";
    // oracle获取所有的schema下所有表 ： 新sql
    private static final String SHOW_ALL_TABLE_SQL = "SELECT '\"'||OWNER||'\"'||'.'||'\"'||TABLE_NAME||'\"' AS TABLE_NAME FROM ALL_TABLES WHERE OWNER != 'SYS' %s ";
    // oracle获取所有的schema下所有视图 ： 新sql
    private static final String SHOW_ALL_VIEW_SQL = " UNION SELECT '\"'||OWNER||'\"'||'.'||'\"'||VIEW_NAME||'\"' AS TABLE_NAME FROM ALL_VIEWS WHERE OWNER != 'SYS' %s ";
    // 表查询基础sql
    private static final String TABLE_BASE_SQL = "SELECT TABLE_NAME FROM (%s) WHERE 1 = 1 %s ";
    // 表名正则匹配模糊查询，忽略大小写
    private static final String TABLE_SEARCH_SQL = " AND REGEXP_LIKE (TABLE_NAME, '%s', 'i') ";
    // 视图正则匹配模糊查询，忽略大小写
    private static final String VIEW_SEARCH_SQL = " AND REGEXP_LIKE (VIEW_NAME, '%s', 'i') ";
    // 限制条数语句
    private static final String LIMIT_SQL = " AND ROWNUM <= %s ";
    /* ----------------------------------------------------------------------------------------- */

    @Override
    protected ConnFactory getConnFactory() {
        return new OracleConnFactory();
    }

    @Override
    protected DataSourceType getSourceType() {
        return DataSourceType.Oracle;
    }

    @Override
    public List<String> getTableList(ISourceDTO iSource, SqlQueryDTO queryDTO) throws Exception {
        OracleSourceDTO oracleSourceDTO = (OracleSourceDTO) iSource;
        Integer clearStatus = beforeQuery(oracleSourceDTO, queryDTO, false);

        Statement statement = null;
        ResultSet rs = null;
        List<String> tableList = new ArrayList<>();
        try {
            String sql = queryDTO != null && queryDTO.getView() ? ORACLE_ALL_TABLES_SQL + ORACLE_WITH_VIEWS_SQL :
                    ORACLE_ALL_TABLES_SQL;
            statement = oracleSourceDTO.getConnection().createStatement();
            rs = statement.executeQuery(sql);
            while (rs.next()) {
                tableList.add(rs.getString(1));
            }
        } catch (Exception e) {
            throw new DtLoaderException("获取表异常", e);
        } finally {
            DBUtil.closeDBResources(rs, statement, oracleSourceDTO.clearAfterGetConnection(clearStatus));
        }
        return tableList;
    }

    @Override
    public String getTableMetaComment(ISourceDTO iSource, SqlQueryDTO queryDTO) throws Exception {
        OracleSourceDTO oracleSourceDTO = (OracleSourceDTO) iSource;
        Integer clearStatus = beforeColumnQuery(oracleSourceDTO, queryDTO);

        String tableName = queryDTO.getTableName();
        if (tableName.contains(".")) {
            tableName = tableName.split("\\.")[1];
        }
        tableName = tableName.replace("\"", "");

        Statement statement = null;
        ResultSet resultSet = null;

        try {
            DatabaseMetaData metaData = oracleSourceDTO.getConnection().getMetaData();
            resultSet = metaData.getTables(null, null, tableName, null);
            while (resultSet.next()) {
                String comment = resultSet.getString(DtClassConsistent.PublicConsistent.REMARKS);
                return comment;
            }
        } catch (Exception e) {
            throw new DtLoaderException(String.format("获取表:%s 的信息时失败. 请联系 DBA 核查该库、表信息.",
                    queryDTO.getTableName()), e);
        } finally {
            DBUtil.closeDBResources(resultSet, statement, oracleSourceDTO.clearAfterGetConnection(clearStatus));
        }
        return "";
    }

    @Override
    public List<ColumnMetaDTO> getFlinkColumnMetaData(ISourceDTO iSource, SqlQueryDTO queryDTO) throws Exception {
        OracleSourceDTO oracleSourceDTO = (OracleSourceDTO) iSource;
        Integer clearStatus = beforeColumnQuery(oracleSourceDTO, queryDTO);
        Statement statement = null;
        ResultSet rs = null;
        List<ColumnMetaDTO> columns = new ArrayList<>();
        try {
            statement = oracleSourceDTO.getConnection().createStatement();
            String schemaTable = transferSchemaAndTableName(oracleSourceDTO.getSchema(), queryDTO.getTableName());
            String queryColumnSql = "select " + CollectionUtil.listToStr(queryDTO.getColumns()) + " from " + schemaTable
                        + " where 1=2";
            rs = statement.executeQuery(queryColumnSql);
            ResultSetMetaData rsMetaData = rs.getMetaData();
            for (int i = 0, len = rsMetaData.getColumnCount(); i < len; i++) {
                ColumnMetaDTO columnMetaDTO = new ColumnMetaDTO();
                columnMetaDTO.setKey(rsMetaData.getColumnName(i + 1));
                String flinkSqlType = OracleDbAdapter.mapColumnTypeJdbc2Java(rsMetaData.getColumnType(i + 1), rsMetaData.getPrecision(i + 1), rsMetaData.getScale(i + 1));
                if (StringUtils.isNotEmpty(flinkSqlType)) {
                    columnMetaDTO.setType(flinkSqlType);
                }
                columnMetaDTO.setPart(false);
                // 获取字段精度
                if (columnMetaDTO.getType().equalsIgnoreCase("decimal")
                        || columnMetaDTO.getType().equalsIgnoreCase("float")
                        || columnMetaDTO.getType().equalsIgnoreCase("double")
                        || columnMetaDTO.getType().equalsIgnoreCase("numeric")) {
                    columnMetaDTO.setScale(rsMetaData.getScale(i + 1));
                    columnMetaDTO.setPrecision(rsMetaData.getPrecision(i + 1));
                }

                columns.add(columnMetaDTO);
            }
            return columns;

        } catch (SQLException e) {
            if (e.getMessage().contains(DONT_EXIST)) {
                throw new DtLoaderException(queryDTO.getTableName() + "表不存在", e);
            } else {
                throw new DtLoaderException(String.format("获取表:%s 的字段的元信息时失败. 请联系 DBA 核查该库、表信息.",
                        queryDTO.getTableName()), e);
            }
        } finally {
            DBUtil.closeDBResources(rs, statement, oracleSourceDTO.clearAfterGetConnection(clearStatus));
        }
    }

    @Override
    protected Map<String, String> getColumnComments(RdbmsSourceDTO sourceDTO, SqlQueryDTO queryDTO) throws Exception {
        Integer clearStatus = beforeColumnQuery(sourceDTO, queryDTO);
        Statement statement = null;
        ResultSet rs = null;
        Map<String, String> columnComments = new HashMap<>();
        try {
            statement = sourceDTO.getConnection().createStatement();
            String queryColumnCommentSql =
                    "select * from all_col_comments where Table_Name =" + addSingleQuotes(queryDTO.getTableName());
            rs = statement.executeQuery(queryColumnCommentSql);
            while (rs.next()) {
                String columnName = rs.getString(ORACLE_COLUMN_NAME);
                String columnComment = rs.getString(ORACLE_COLUMN_COMMENT);
                columnComments.put(columnName, columnComment);
            }

        } catch (Exception e) {
            if (e.getMessage().contains(DONT_EXIST)) {
                throw new DtLoaderException(queryDTO.getTableName() + "表不存在", e);
            } else {
                throw new DtLoaderException(String.format("获取表:%s 的字段的注释信息时失败. 请联系 DBA 核查该库、表信息.",
                        queryDTO.getTableName()), e);
            }
        }finally {
            DBUtil.closeDBResources(rs, statement, sourceDTO.clearAfterGetConnection(clearStatus));
        }
        return columnComments;
    }

    private static String addSingleQuotes(String str) {
        str = str.contains("'") ? str : String.format("'%s'", str);
        return str;
    }

    @Override
    protected String doDealType(ResultSetMetaData rsMetaData, Integer los) throws SQLException {
        String type = super.doDealType(rsMetaData, los);
        if (!(rsMetaData instanceof OracleResultSetMetaData) || !ORACLE_NUMBER_TYPE.equalsIgnoreCase(type)) {
            return type;
        }

        int precision = rsMetaData.getPrecision(los + 1);
        int scale = rsMetaData.getScale(los + 1);
        // fixme float类型返回 p 126 ；s -127。正常来说，p的范围1——38；s的范围是-84——127。更倾向于认为是jdbc的一个bug（未验证）
        if (precision == 126 && scale == -127) {
            return "FLOAT";
        }
        if (precision == 0 && scale < 0) {
            return ORACLE_NUMBER_TYPE;
        }

        return String.format(ORACLE_NUMBER_FORMAT, precision, scale);
    }

    @Override
    public IDownloader getDownloader(ISourceDTO source, SqlQueryDTO queryDTO) throws Exception {
        OracleSourceDTO oracleSourceDTO = (OracleSourceDTO) source;
        OracleDownloader oracleDownloader = new OracleDownloader(getCon(oracleSourceDTO), queryDTO.getSql(), oracleSourceDTO.getSchema());
        oracleDownloader.configure();
        return oracleDownloader;
    }

    @Override
    protected String dealSql(ISourceDTO iSourceDTO, SqlQueryDTO sqlQueryDTO){
        OracleSourceDTO oracleSourceDTO = (OracleSourceDTO) iSourceDTO;
        return "select * from " + transferSchemaAndTableName(oracleSourceDTO.getSchema(), sqlQueryDTO.getTableName()) + " where rownum <=" + sqlQueryDTO.getPreviewNum();
    }

    /**
     * 查询指定schema下的表，如果没有填schema，默认使用当前schema：支持条数限制、正则匹配
     *
     * @param sourceDTO 数据源信息
     * @param queryDTO 查询条件
     * @return 对应的sql语句
     */
    @Override
    protected String getTableBySchemaSql(ISourceDTO sourceDTO, SqlQueryDTO queryDTO) {
        // 构造表名模糊查询和条数限制sql
        String tableConstr = buildSearchSql(TABLE_SEARCH_SQL, queryDTO.getTableNamePattern(), queryDTO.getLimit());
        // 构造视图模糊查询和条数限制sql
        String viewConstr = buildSearchSql(VIEW_SEARCH_SQL, queryDTO.getTableNamePattern(), queryDTO.getLimit());
        String schema = queryDTO.getSchema();
        // schema若为空，则查询所有schema下的表
        String searchSql;
        if (StringUtils.isBlank(schema)) {
            log.info("schema为空，获取所有表！");
            searchSql = queryDTO.getView() ? String.format(SHOW_ALL_TABLE_SQL + SHOW_ALL_VIEW_SQL, tableConstr, viewConstr) : String.format(SHOW_ALL_TABLE_SQL, tableConstr);
        } else {
            searchSql = queryDTO.getView() ?  String.format(SHOW_TABLE_BY_SCHEMA_SQL + SHOW_VIEW_BY_SCHEMA_SQL, schema, tableConstr, schema, viewConstr) : String.format(SHOW_TABLE_BY_SCHEMA_SQL, schema, tableConstr);
        }
        log.info("当前使用schema：{}", schema);

        return String.format(TABLE_BASE_SQL, searchSql, tableConstr);
    }

    /**
     * 构造模糊查询、条数限制sql
     * @param tableSearchSql
     * @param tableNamePattern
     * @param limit
     * @return
     */
    private String buildSearchSql(String tableSearchSql, String tableNamePattern, Integer limit) {
        StringBuilder constr = new StringBuilder();
        if (StringUtils.isNotBlank(tableNamePattern)) {
            constr.append(String.format(tableSearchSql, tableNamePattern));
        }
        if (Objects.nonNull(limit)) {
            constr.append(String.format(LIMIT_SQL, limit));
        }
        return constr.toString();
    }

    @Override
    public String getCreateTableSql(ISourceDTO source, SqlQueryDTO queryDTO) throws Exception {
        OracleSourceDTO oracleSourceDTO = (OracleSourceDTO) source;
        String createTableSql = String.format(TABLE_CREATE_SQL,queryDTO.getTableName(),oracleSourceDTO.getSchema());
        queryDTO.setSql(createTableSql);
        return super.getCreateTableSql(source, queryDTO);
    }

    @Override
    public List<ColumnMetaDTO> getPartitionColumn(ISourceDTO source, SqlQueryDTO queryDTO) throws Exception {
        throw new DtLoaderException("Not Support");
    }

    @Override
    public String getShowDbSql() {
        return DATABASE_QUERY;
    }

    /**
     * 处理Oracle schema和tableName，适配schema和tableName中有.的情况
     * @param schema
     * @param tableName
     * @return
     */
    @Override
    protected String transferSchemaAndTableName(String schema, String tableName) {
        if (!tableName.startsWith("\"") || !tableName.endsWith("\"")) {
            tableName = String.format("\"%s\"", tableName);
        }
        if (StringUtils.isBlank(schema)) {
            return tableName;
        }
        if (!schema.startsWith("\"") || !schema.endsWith("\"")){
            schema = String.format("\"%s\"", schema);
        }
        return String.format("%s.%s", schema, tableName);
    }

    @Override
    protected String getCurrentDbSql() {
        return CURRENT_DB;
    }

}
