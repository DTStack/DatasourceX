package com.dtstack.dtcenter.common.loader.sqlserver;

import com.dtstack.dtcenter.common.loader.common.DtClassConsistent;
import com.dtstack.dtcenter.common.loader.common.utils.DBUtil;
import com.dtstack.dtcenter.common.loader.rdbms.AbsRdbmsClient;
import com.dtstack.dtcenter.common.loader.rdbms.ConnFactory;
import com.dtstack.dtcenter.loader.IDownloader;
import com.dtstack.dtcenter.loader.dto.ColumnMetaDTO;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;
import com.dtstack.dtcenter.loader.dto.source.RdbmsSourceDTO;
import com.dtstack.dtcenter.loader.dto.source.SqlserverSourceDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import org.apache.commons.lang3.StringUtils;

import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 15:30 2020/1/7
 * @Description：SqlServer 客户端
 */
public class SqlServerClient extends AbsRdbmsClient {
    private static final String TABLE_QUERY_ALL = "select table_name, table_schema from information_schema.tables where table_type in ('VIEW', 'BASE TABLE')";
    private static final String TABLE_QUERY = "select table_name, table_schema from information_schema.tables where table_type in ('BASE TABLE')";

    private static final String SEARCH_BY_COLUMN_SQL = " and charIndex('%s', a.name) > 0 ";

    private static final String TABLE_SHOW = "[%s].[%s]";

    // 获取正在使用数据库
    private static final String CURRENT_DB = "Select Name From Master..SysDataBases Where DbId=(Select Dbid From Master..SysProcesses Where Spid = @@spid)";

    private static final String SEARCH_LIMIT_SQL = "select top %s table_name from information_schema.tables where 1=1";
    private static final String SEARCH_SQL = "select table_name from information_schema.tables where 1=1";
    private static final String SCHEMA_SQL = " and table_schema='%s'";
    private static final String TABLE_NAME_SQL = " and charIndex('%s',table_name) > 0";

    private static final String SCHEMAS_QUERY = "select DISTINCT(name) as schema_name from sys.schemas where schema_id < 16384";
    private static String SQL_SERVER_COLUMN_NAME = "column_name";
    private static String SQL_SERVER_COLUMN_COMMENT = "column_description";
    private static final String COMMENT_QUERY = "SELECT B.name AS column_name, C.value AS column_description FROM sys.tables A INNER JOIN sys.columns B ON B.object_id = A.object_id LEFT JOIN sys.extended_properties C ON C.major_id = B.object_id AND C.minor_id = B.column_id WHERE A.name = N";

    // 获取当前版本号
    private static final String SHOW_VERSION = "SELECT @@VERSION";

    // 创建 schema
    private static final String CREATE_SCHEMA_SQL_TMPL = "create schema %s ";

    @Override
    protected ConnFactory getConnFactory() {
        return new SQLServerConnFactory();
    }

    @Override
    protected DataSourceType getSourceType() {
        return DataSourceType.SQLServer;
    }

    @Override
    public List<String> getTableList(ISourceDTO iSource, SqlQueryDTO queryDTO) {
        SqlserverSourceDTO sqlserverSourceDTO = (SqlserverSourceDTO) iSource;
        String schema = StringUtils.isNotBlank(queryDTO.getSchema()) ? queryDTO.getSchema() : sqlserverSourceDTO.getSchema();
        Integer clearStatus = beforeQuery(sqlserverSourceDTO, queryDTO, false);

        Statement statement = null;
        ResultSet rs = null;
        List<String> tableList = new ArrayList<>();
        try {
            String sql = queryDTO.getView() ? TABLE_QUERY_ALL : TABLE_QUERY;
            if (StringUtils.isNotBlank(queryDTO.getTableNamePattern())) {
                sql = sql + String.format(SEARCH_BY_COLUMN_SQL, queryDTO.getTableNamePattern());
            }
            // 查询schema下的
            if (StringUtils.isNotBlank(schema)) {
                sql += String.format(SCHEMA_SQL, schema);
            }
            statement = sqlserverSourceDTO.getConnection().createStatement();
            if (Objects.nonNull(queryDTO.getLimit())) {
                statement.setMaxRows(queryDTO.getLimit());
            }
            DBUtil.setFetchSize(statement, queryDTO);
            rs = statement.executeQuery(sql);
            while (rs.next()) {
                String tableName = StringUtils.isNotBlank(schema) ? rs.getString(1) :
                        String.format(TABLE_SHOW, rs.getString(2), rs.getString(1));
                tableList.add(tableName);
            }
        } catch (Exception e) {
            throw new DtLoaderException(String.format("get table exception,%s", e.getMessage()), e);
        } finally {
            DBUtil.closeDBResources(rs, statement, DBUtil.clearAfterGetConnection(sqlserverSourceDTO, clearStatus));
        }
        return tableList;
    }

    @Override
    public String getTableMetaComment(ISourceDTO iSource, SqlQueryDTO queryDTO) {
        SqlserverSourceDTO sqlserverSourceDTO = (SqlserverSourceDTO) iSource;
        Integer clearStatus = beforeColumnQuery(sqlserverSourceDTO, queryDTO);

        Statement statement = null;
        ResultSet resultSet = null;
        try {
            statement = sqlserverSourceDTO.getConnection().createStatement();
            resultSet = statement.executeQuery(
                    "select c.name, cast(isnull(f.[value], '') as nvarchar(100)) as REMARKS\n" +
                    "from sys.objects c " +
                    "left join sys.extended_properties f on f.major_id = c.object_id and f.minor_id = 0 and f.class = 1\n" +
                    "where c.type = 'u'");
            while (resultSet.next()) {
                String dbTableName = resultSet.getString(1);
                if (dbTableName.equalsIgnoreCase(queryDTO.getTableName())) {
                    return resultSet.getString(DtClassConsistent.PublicConsistent.REMARKS);
                }
            }
        } catch (Exception e) {
            throw new DtLoaderException(String.format("get table: %s's information error. Please contact the DBA to check the database、table information.",
                    queryDTO.getTableName()), e);
        } finally {
            DBUtil.closeDBResources(resultSet, statement, DBUtil.clearAfterGetConnection(sqlserverSourceDTO, clearStatus));
        }
        return null;
    }

    @Override
    public IDownloader getDownloader(ISourceDTO source, SqlQueryDTO queryDTO) throws Exception {
        SqlserverSourceDTO sqlserverSourceDTO = (SqlserverSourceDTO) source;
        String schema = StringUtils.isNotBlank(queryDTO.getSchema()) ? queryDTO.getSchema() : sqlserverSourceDTO.getSchema();
        SqlServerDownloader sqlServerDownloader = new SqlServerDownloader(getCon(sqlserverSourceDTO), queryDTO.getSql(), schema);
        sqlServerDownloader.configure();
        return sqlServerDownloader;
    }

    @Override
    protected String transferSchemaAndTableName(String schema, String tableName) {
        //如果传过来是[tableName]格式直接当成表名
        if (tableName.startsWith("[") && tableName.endsWith("]")){
            if (StringUtils.isNotBlank(schema)) {
                return String.format("%s.%s", schema, tableName);
            }
            return tableName;
        }
        //如果不是上述格式，判断有没有"."符号，有的话，第一个"."之前的当成schema，后面的当成表名进行[tableName]处理
        if (tableName.contains(".")) {
            //切割，表名中可能会有包含"."的情况，所以限制切割后长度为2
            String[] tables = tableName.split("\\.", 2);
            tableName = tables[1];
            return String.format("%s.%s", tables[0], tableName.contains("[") ? tableName : String.format("[%s]",
                    tableName));
        }
        //判断表名
        if (StringUtils.isNotBlank(schema)) {
            return String.format("%s.[%s]", schema, tableName);
        }
        return String.format("[%s]", tableName);
    }

    @Override
    protected String getTableBySchemaSql(ISourceDTO sourceDTO, SqlQueryDTO queryDTO) {
        StringBuilder constr = new StringBuilder();
        if(queryDTO.getLimit() != null) {
            constr.append(String.format(SEARCH_LIMIT_SQL, queryDTO.getLimit()));
        }else {
            constr.append(SEARCH_SQL);
        }
        //判断是否需要schema
        if(StringUtils.isNotBlank(queryDTO.getSchema())){
            constr.append(String.format(SCHEMA_SQL, queryDTO.getSchema()));
        }
        // 根据name 模糊查询
        if(StringUtils.isNotBlank(queryDTO.getTableNamePattern())){
            constr.append(String.format(TABLE_NAME_SQL, queryDTO.getTableNamePattern()));
        }
        return constr.toString();
    }

    @Override
    public String getCreateTableSql(ISourceDTO source, SqlQueryDTO queryDTO) {
        throw new DtLoaderException("Not Support");
    }

    @Override
    public List<ColumnMetaDTO> getPartitionColumn(ISourceDTO source, SqlQueryDTO queryDTO) {
        throw new DtLoaderException("Not Support");
    }

    @Override
    protected Map<String, String> getColumnComments(RdbmsSourceDTO sourceDTO, SqlQueryDTO queryDTO) {
        Integer clearStatus = beforeColumnQuery(sourceDTO, queryDTO);
        Statement statement = null;
        ResultSet rs = null;
        Map<String, String> columnComments = new HashMap<>();
        try {
            statement = sourceDTO.getConnection().createStatement();
            String queryColumnCommentSql = COMMENT_QUERY + addSingleQuotes(queryDTO.getTableName());
            rs = statement.executeQuery(queryColumnCommentSql);
            while (rs.next()) {
                String columnName = rs.getString(SQL_SERVER_COLUMN_NAME);
                String columnComment = rs.getString(SQL_SERVER_COLUMN_COMMENT);
                columnComments.put(columnName, columnComment);
            }

        } catch (Exception e) {
            //获取表字段注释失败
        }finally {
            DBUtil.closeDBResources(rs, statement, DBUtil.clearAfterGetConnection(sourceDTO, clearStatus));
        }
        return columnComments;
    }

    private static String addSingleQuotes(String str) {
        str = str.contains("'") ? str : String.format("'%s'", str);
        return str;
    }

    @Override
    public String getShowDbSql() {
        return SCHEMAS_QUERY;
    }

    @Override
    protected String getCurrentDbSql() {
        return CURRENT_DB;
    }

    @Override
    public String getVersion(ISourceDTO source) {
        return SHOW_VERSION;
    }

    @Override
    protected String getCreateDatabaseSql(String dbName, String comment) {
        return String.format(CREATE_SCHEMA_SQL_TMPL, dbName);
    }
}
