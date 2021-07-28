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
import com.dtstack.dtcenter.loader.dto.source.Sqlserver2017SourceDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import org.apache.commons.lang3.StringUtils;

import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 15:30 2020/1/7
 * @Description：SqlServer 客户端
 */
public class SqlServerClient extends AbsRdbmsClient {
    private static final String TABLE_QUERY_ALL = "select * from sys.objects where type='U' or type='V'";
    private static final String TABLE_QUERY = "select * from sys.objects where type='U'";

    private static String SQL_SERVER_COLUMN_NAME = "column_name";
    private static String SQL_SERVER_COLUMN_COMMENT = "column_description";
    private static final String SCHEMAS_QUERY = "select distinct(sys.schemas.name) as schema_name from sys.objects,sys.schemas where sys.objects.type='U' and sys.objects.schema_id=sys.schemas.schema_id";
    private static final String COMMENT_QUERY = "SELECT B.name AS column_name, C.value AS column_description FROM sys.tables A INNER JOIN sys.columns B ON B.object_id = A.object_id LEFT JOIN sys.extended_properties C ON C.major_id = B.object_id AND C.minor_id = B.column_id WHERE A.name = N";
    // 获取正在使用数据库
    private static final String CURRENT_DB = "Select Name From Master..SysDataBases Where DbId=(Select Dbid From Master..SysProcesses Where Spid = @@spid)";
    /**
     * 根据schema获取对应的表：开启cdc的表
     */
    private static final String TABLE_BY_SCHEMA = "SELECT sys.tables.name AS table_name,sys.schemas.name AS schema_name \n" +
            "FROM sys.tables LEFT JOIN sys.schemas ON sys.tables.schema_id=sys.schemas.schema_id \n" +
            "WHERE sys.tables.type='U' AND sys.tables.is_tracked_by_cdc =1\n" +
            "AND sys.schemas.name = '%s'";

    @Override
    protected ConnFactory getConnFactory() {
        return new SQLServerConnFactory();
    }

    @Override
    protected DataSourceType getSourceType() {
        return DataSourceType.SQLSERVER_2017_LATER;
    }

    @Override
    public List<String> getTableList(ISourceDTO iSource, SqlQueryDTO queryDTO) {
        Sqlserver2017SourceDTO sqlserver2017SourceDTO = (Sqlserver2017SourceDTO) iSource;
        Integer clearStatus = beforeQuery(sqlserver2017SourceDTO, queryDTO, false);

        Statement statement = null;
        ResultSet rs = null;
        List<String> tableList = new ArrayList<>();
        try {
            String sql = queryDTO.getView() ? TABLE_QUERY_ALL : TABLE_QUERY;
            statement = sqlserver2017SourceDTO.getConnection().createStatement();
            DBUtil.setFetchSize(statement, queryDTO);
            rs = statement.executeQuery(sql);
            int columnSize = rs.getMetaData().getColumnCount();
            while (rs.next()) {
                tableList.add(rs.getString(1));
            }
        } catch (Exception e) {
            throw new DtLoaderException(String.format("get table exception,%s", e.getMessage()), e);
        } finally {
            DBUtil.closeDBResources(rs, statement, DBUtil.clearAfterGetConnection(sqlserver2017SourceDTO, clearStatus));
        }
        return tableList;
    }

    @Override
    public String getTableMetaComment(ISourceDTO iSource, SqlQueryDTO queryDTO) {
        Sqlserver2017SourceDTO sqlserver2017SourceDTO = (Sqlserver2017SourceDTO) iSource;
        Integer clearStatus = beforeColumnQuery(sqlserver2017SourceDTO, queryDTO);

        Statement statement = null;
        ResultSet resultSet = null;
        try {
            statement = sqlserver2017SourceDTO.getConnection().createStatement();
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
            DBUtil.closeDBResources(resultSet, statement, DBUtil.clearAfterGetConnection(sqlserver2017SourceDTO, clearStatus));
        }
        return "";
    }

    @Override
    public IDownloader getDownloader(ISourceDTO source, SqlQueryDTO queryDTO) throws Exception {
        Sqlserver2017SourceDTO sqlserver2017SourceDTO = (Sqlserver2017SourceDTO) source;
        SqlServerDownloader sqlServerDownloader = new SqlServerDownloader(getCon(sqlserver2017SourceDTO), queryDTO.getSql(), sqlserver2017SourceDTO.getSchema());
        sqlServerDownloader.configure();
        return sqlServerDownloader;
    }

    @Override
    protected String dealSql(ISourceDTO source, SqlQueryDTO sqlQueryDTO) {
        return "select top " + sqlQueryDTO.getPreviewNum() + " * from " + transferSchemaAndTableName(source, sqlQueryDTO);
    }

    @Override
    protected String transferSchemaAndTableName(String schema, String tableName) {
        if (StringUtils.isBlank(schema)) {
            return transferTableName(tableName);
        }
        if (!tableName.startsWith("[") || !tableName.endsWith("]")) {
            tableName = String.format("[%s]", tableName);
        }
        if (!schema.startsWith("[") || !schema.endsWith("]")){
            schema = String.format("[%s]", schema);
        }
        return String.format("%s.%s", schema, tableName);
    }

    @Override
    protected String transferTableName(String tableName) {
        //如果传过来是[tableName]格式直接当成表名
        if (tableName.startsWith("[") && tableName.endsWith("]")){
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
        return String.format("[%s]", tableName);
    }

    @Override
    protected String getTableBySchemaSql(ISourceDTO sourceDTO, SqlQueryDTO queryDTO) {
        return String.format(TABLE_BY_SCHEMA, queryDTO.getSchema());
    }

    @Override
    public String getCreateTableSql(ISourceDTO source, SqlQueryDTO queryDTO)  {
        throw new DtLoaderException("Not Support");
    }

    @Override
    public List<ColumnMetaDTO> getPartitionColumn(ISourceDTO source, SqlQueryDTO queryDTO)  {
        throw new DtLoaderException("Not Support");
    }

    @Override
    protected Map<String, String> getColumnComments(RdbmsSourceDTO sourceDTO, SqlQueryDTO queryDTO)  {
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
}
