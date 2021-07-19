package com.dtstack.dtcenter.common.loader.oracle;

import com.dtstack.dtcenter.common.loader.common.DtClassConsistent;
import com.dtstack.dtcenter.common.loader.common.utils.CollectionUtil;
import com.dtstack.dtcenter.common.loader.common.utils.DBUtil;
import com.dtstack.dtcenter.common.loader.common.utils.ReflectUtil;
import com.dtstack.dtcenter.common.loader.rdbms.AbsRdbmsClient;
import com.dtstack.dtcenter.common.loader.rdbms.ConnFactory;
import com.dtstack.dtcenter.loader.IDownloader;
import com.dtstack.dtcenter.loader.dto.ColumnMetaDTO;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;
import com.dtstack.dtcenter.loader.dto.source.OracleSourceDTO;
import com.dtstack.dtcenter.loader.dto.source.RdbmsSourceDTO;
import com.dtstack.dtcenter.loader.enums.ConnectionClearStatus;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import lombok.extern.slf4j.Slf4j;
import oracle.jdbc.OracleResultSetMetaData;
import oracle.sql.BLOB;
import oracle.sql.CLOB;
import oracle.xdb.XMLType;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;

import java.sql.Connection;
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
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 12:00 2020/1/6
 * @Description：Oracle 客户端
 */
@Slf4j
public class OracleClient extends AbsRdbmsClient {

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

    // 获取 oracle PDB 列表前 设置 session
    private static final String ALTER_PDB_SESSION = "ALTER SESSION SET CONTAINER=%s";

    // cdb root
    private static final String CDB_ROOT = "CDB$ROOT";

    // 获取 oracle PDB 列表
    private static final String LIST_PDB = "SELECT NAME FROM v$pdbs WHERE 1 = 1 %s";

    // 表名正则匹配模糊查询，忽略大小写
    private static final String PDB_SEARCH_SQL = " AND REGEXP_LIKE (NAME, '%s', 'i') ";

    // 获取当前版本号
    private static final String SHOW_VERSION = "select BANNER from v$version";

    @Override
    protected ConnFactory getConnFactory() {
        return new OracleConnFactory();
    }

    @Override
    protected DataSourceType getSourceType() {
        return DataSourceType.Oracle;
    }

    @Override
    public List<String> getTableList(ISourceDTO sourceDTO, SqlQueryDTO queryDTO) {
        return getTableListBySchema(sourceDTO, queryDTO);
    }

    @Override
    public String getTableMetaComment(ISourceDTO iSource, SqlQueryDTO queryDTO) {
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
            throw new DtLoaderException(String.format("get table: %s's information error. Please contact the DBA to check the database、table information.",
                    queryDTO.getTableName()), e);
        } finally {
            DBUtil.closeDBResources(resultSet, statement, DBUtil.clearAfterGetConnection(oracleSourceDTO, clearStatus));
        }
        return "";
    }

    @Override
    public List<ColumnMetaDTO> getFlinkColumnMetaData(ISourceDTO iSource, SqlQueryDTO queryDTO) {
        OracleSourceDTO oracleSourceDTO = (OracleSourceDTO) iSource;
        Integer clearStatus = beforeColumnQuery(oracleSourceDTO, queryDTO);
        Statement statement = null;
        ResultSet rs = null;
        List<ColumnMetaDTO> columns = new ArrayList<>();
        try {
            statement = oracleSourceDTO.getConnection().createStatement();
            String schemaTable = transferSchemaAndTableName(oracleSourceDTO, queryDTO);
            String queryColumnSql = "select " + CollectionUtil.listToStr(queryDTO.getColumns()) + " from " + schemaTable
                        + " where 1=2";
            rs = statement.executeQuery(queryColumnSql);
            ResultSetMetaData rsMetaData = rs.getMetaData();
            for (int i = 0, len = rsMetaData.getColumnCount(); i < len; i++) {
                ColumnMetaDTO columnMetaDTO = new ColumnMetaDTO();
                columnMetaDTO.setKey(rsMetaData.getColumnName(i + 1));
                String flinkSqlType = OracleDbAdapter.mapColumnTypeJdbc2Java(rsMetaData.getColumnType(i + 1), rsMetaData.getPrecision(i + 1), rsMetaData.getScale(i + 1));
                if (StringUtils.isBlank(flinkSqlType)) {
                    throw new DtLoaderException(String.format("oracle not support %s type fields's collection", rsMetaData.getColumnTypeName(i + 1)));
                }
                columnMetaDTO.setType(flinkSqlType);
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

            //获取字段注释
            Map<String, String> columnComments = getColumnComments(oracleSourceDTO, queryDTO);
            if (Objects.isNull(columnComments)) {
                return columns;
            }
            for (ColumnMetaDTO columnMetaDTO : columns) {
                if (columnComments.containsKey(columnMetaDTO.getKey())) {
                    columnMetaDTO.setComment(columnComments.get(columnMetaDTO.getKey()));
                }
            }
            return columns;

        } catch (SQLException e) {
            if (e.getMessage().contains(DONT_EXIST)) {
                throw new DtLoaderException(String.format(queryDTO.getTableName() + "table not exist,%s", e.getMessage()), e);
            } else {
                throw new DtLoaderException(String.format("Failed to get meta information for the fields of table :%s. Please contact the DBA to check the database table information.",
                        queryDTO.getTableName()), e);
            }
        } finally {
            DBUtil.closeDBResources(rs, statement, DBUtil.clearAfterGetConnection(oracleSourceDTO, clearStatus));
        }
    }

    @Override
    protected Map<String, String> getColumnComments(RdbmsSourceDTO sourceDTO, SqlQueryDTO queryDTO) {
        Integer clearStatus = beforeColumnQuery(sourceDTO, queryDTO);
        Statement statement = null;
        ResultSet rs = null;
        Map<String, String> columnComments = new HashMap<>();
        try {
            statement = sourceDTO.getConnection().createStatement();
            String queryColumnCommentSql =
                    "select * from all_col_comments where Table_Name =" + addSingleQuotes(getTableName(sourceDTO.getSchema(), queryDTO.getTableName()));
            rs = statement.executeQuery(queryColumnCommentSql);
            while (rs.next()) {
                String columnName = rs.getString(ORACLE_COLUMN_NAME);
                String columnComment = rs.getString(ORACLE_COLUMN_COMMENT);
                columnComments.put(columnName, columnComment);
            }

        } catch (Exception e) {
            if (e.getMessage().contains(DONT_EXIST)) {
                throw new DtLoaderException(String.format(queryDTO.getTableName() + "table not exist,%s", e.getMessage()), e);
            } else {
                throw new DtLoaderException(String.format("Failed to get the comment information of the field of the table: %s. Please contact the DBA to check the database and table information.",
                        queryDTO.getTableName()), e);
            }
        }finally {
            DBUtil.closeDBResources(rs, statement, DBUtil.clearAfterGetConnection(sourceDTO, clearStatus));
        }
        return columnComments;
    }

    /**
     * 添加单引号
     *
     * @param str
     * @return
     */
    private static String addSingleQuotes(String str) {
        str = str.contains("'") ? str : String.format("'%s'", str);
        return str;
    }

    /**
     * 获取表名
     *
     * @param schema
     * @param tableName
     * @return
     */
    private String getTableName(String schema, String tableName) {
        String schemaAndTableName = transferSchemaAndTableName(schema, tableName);
        List<String> splitWithQuotations = splitWithQuotation(schemaAndTableName, "\"");
        if (splitWithQuotations.size() != 2) {
            return tableName;
        }
        return splitWithQuotations.get(1);
    }

    /**
     * 获取Schema名字
     *
     * @param schema
     * @param tableName
     * @return
     */
    private String getSchemaName(String schema, String tableName) {
        String schemaAndTableName = transferSchemaAndTableName(schema, tableName);
        List<String> splitWithQuotations = splitWithQuotation(schemaAndTableName, "\"");
        if (splitWithQuotations.isEmpty()) {
            return schema;
        }
        return splitWithQuotations.get(0);
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
        return "select * from " + transferSchemaAndTableName(oracleSourceDTO, sqlQueryDTO) + " where rownum <=" + sqlQueryDTO.getPreviewNum();
    }

    @Override
    protected Object dealResult(Object result) {
        if (result instanceof XMLType) {
            try {
                XMLType xmlResult = (XMLType) result;
                return xmlResult.getString();
            } catch (Exception e) {
                log.error("oracle xml format transform string exception！", e);
                return "";
            }
        }

        // 处理 Blob 字段
        if (result instanceof BLOB) {
            try {
                BLOB blobResult = (BLOB) result;
                return blobResult.toString();
            } catch (Exception e) {
                log.error("oracle Blob format transform String exception！", e);
                return "";
            }
        }

        // 处理 Clob 字段
        if (result instanceof CLOB) {
            CLOB clobResult = (CLOB) result;
            try {
                return clobResult.toSQLXML().getString();
            } catch (Exception e) {
                log.error("oracle Clob format transform String exception！", e);
                return "";
            }
        }
        return result;
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
            log.info("schema is null，get all table！");
            searchSql = queryDTO.getView() ? String.format(SHOW_ALL_TABLE_SQL + SHOW_ALL_VIEW_SQL, tableConstr, viewConstr) : String.format(SHOW_ALL_TABLE_SQL, tableConstr);
        } else {
            searchSql = queryDTO.getView() ?  String.format(SHOW_TABLE_BY_SCHEMA_SQL + SHOW_VIEW_BY_SCHEMA_SQL, schema, tableConstr, schema, viewConstr) : String.format(SHOW_TABLE_BY_SCHEMA_SQL, schema, tableConstr);
        }
        log.info("current used schema：{}", schema);

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
        if (org.apache.commons.lang3.StringUtils.isNotBlank(tableNamePattern)) {
            constr.append(String.format(tableSearchSql, tableNamePattern));
        }
        if (Objects.nonNull(limit)) {
            constr.append(String.format(LIMIT_SQL, limit));
        }
        return constr.toString();
    }

    @Override
    public String getCreateTableSql(ISourceDTO source, SqlQueryDTO queryDTO) {
        OracleSourceDTO oracleSourceDTO = (OracleSourceDTO) source;
        String createTableSql = String.format(TABLE_CREATE_SQL,queryDTO.getTableName(),oracleSourceDTO.getSchema());
        queryDTO.setSql(createTableSql);
        return super.getCreateTableSql(source, queryDTO);
    }

    @Override
    public List<ColumnMetaDTO> getPartitionColumn(ISourceDTO source, SqlQueryDTO queryDTO) {
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
            // 如果tableName包含Schema操作，无其他方法，只能去判断长度
        } else if (indexCount(tableName, "\"") >= 4) {
            return tableName;
        }
        if (StringUtils.isBlank(schema)) {
            return tableName;
        }
        if (!schema.startsWith("\"") || !schema.endsWith("\"")){
            schema = String.format("\"%s\"", schema);
        }
        return String.format("%s.%s", schema, tableName);
    }

    /**
     * 通过采用indexOf + substring + 递归的方式来获取指定字符的数量
     *
     * @param text      指定要搜索的字符串
     * @param countText 指定要搜索的字符
     */
    private static int indexCount(String text, String countText) {
        // 根据指定的字符构建正则
        Pattern pattern = Pattern.compile(countText);
        // 构建字符串和正则的匹配
        Matcher matcher = pattern.matcher(text);
        int count = 0;
        // 循环依次往下匹配
        // 如果匹配,则数量+1
        while (matcher.find()){
            count++;
        }
        return count;
    }

    /**
     * 正则解析出对应符号内的内容
     *
     * @param text
     * @param quotationText
     * @return
     */
    private static List<String> splitWithQuotation(String text, String quotationText) {
        Pattern quotationPattern = Pattern.compile(quotationText + "(.*?)" + quotationText);
        Matcher quotationMatch = quotationPattern.matcher(text);

        ArrayList<String> results = new ArrayList<>();
        while (quotationMatch.find()) {
            results.add(quotationMatch.group().trim().replace(quotationText, ""));
        }
        return results;
    }

    @Override
    protected String getCurrentDbSql() {
        return CURRENT_DB;
    }

    @Override
    protected Integer beforeQuery(ISourceDTO source, SqlQueryDTO queryDTO, boolean query) {
        OracleSourceDTO oracleSourceDTO = (OracleSourceDTO) source;
        // 版本字段兼容
        Boolean fieldExists = ReflectUtil.fieldExists(OracleSourceDTO.class, "pdb");
        if (!fieldExists || StringUtils.isBlank(oracleSourceDTO.getPdb())) {
            return super.beforeQuery(oracleSourceDTO, queryDTO, query);
        }
        // 如果为查询，SQL 不能为空
        if (query && StringUtils.isBlank(queryDTO.getSql())) {
            throw new DtLoaderException("Query SQL cannot be empty");
        }
        Integer clearStatus;
        // 设置 connection
        if (oracleSourceDTO.getConnection() == null) {
            oracleSourceDTO.setConnection(getCon(oracleSourceDTO));
            clearStatus = ConnectionClearStatus.CLOSE.getValue();
        } else {
            clearStatus = ConnectionClearStatus.NORMAL.getValue();
        }
        // 切换 container session
        alterSession(oracleSourceDTO.getConnection(), oracleSourceDTO.getPdb());
        return clearStatus;
    }

    @Override
    public List<String> getRootDatabases(ISourceDTO source, SqlQueryDTO queryDTO) {
        Integer clearStatus = beforeQuery(source, queryDTO, false);
        OracleSourceDTO oracleSourceDTO = (OracleSourceDTO) source;
        // 执行后 将从 root 获取 PDB，否则获取到的为当前用户所在的 PDB
        try {
            // 构造 pdb 模糊查询和条数限制sql
            String pdbConstr = buildSearchSql(PDB_SEARCH_SQL, queryDTO.getTableNamePattern(), queryDTO.getLimit());
            // 切换到 cdb root，此处不关闭 connection
            DBUtil.executeSqlWithoutResultSet(oracleSourceDTO.getConnection(), String.format(ALTER_PDB_SESSION, CDB_ROOT));
            List<Map<String, Object>> pdbList = executeQuery(oracleSourceDTO, SqlQueryDTO.builder().sql(String.format(LIST_PDB, pdbConstr)).build(), ConnectionClearStatus.NORMAL.getValue());
            return pdbList.stream().map(row -> MapUtils.getString(row, "NAME")).collect(Collectors.toList());
        } catch (Exception e) {
            throw new DtLoaderException(String.format("Error getting PDB list.%s", e.getMessage()), e);
        } finally {
            DBUtil.closeDBResources(null, null, DBUtil.clearAfterGetConnection(oracleSourceDTO, clearStatus));
        }
    }

    /**
     * oracle 切换数据库
     *
     * @param connection 数据库链接
     * @param pdb        pdb
     */
    private void alterSession(Connection connection, String pdb) {
        if (StringUtils.isBlank(pdb)) {
            log.warn("pdb is null, No switching...");
            return;
        }
        try {
            // 切换 pdb session，相当于 mysql 的use db ，此处执行后不关闭 connection
            DBUtil.executeSqlWithoutResultSet(connection, String.format(ALTER_PDB_SESSION, pdb));
        } catch (Exception e) {
            log.error("alter oracle container session error... {}", e.getMessage(), e);
        }
    }

    @Override
    protected String getVersionSql() {
        return SHOW_VERSION;
    }
}
