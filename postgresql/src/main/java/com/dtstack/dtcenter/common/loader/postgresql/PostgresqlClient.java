package com.dtstack.dtcenter.common.loader.postgresql;

import com.dtstack.dtcenter.common.loader.common.utils.DBUtil;
import com.dtstack.dtcenter.common.loader.rdbms.AbsRdbmsClient;
import com.dtstack.dtcenter.common.loader.rdbms.ConnFactory;
import com.dtstack.dtcenter.loader.IDownloader;
import com.dtstack.dtcenter.loader.dto.ColumnMetaDTO;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;
import com.dtstack.dtcenter.loader.dto.source.PostgresqlSourceDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 15:52 2020/1/7
 * @Description：Postgresql 客户端
 */
@Slf4j
public class PostgresqlClient extends AbsRdbmsClient {
    private static final String BIGSERIAL = "bigserial";

    private static final String DATABASE_QUERY = "select nspname from pg_namespace";

    private static final String DONT_EXIST = "doesn't exist";

    // 获取指定schema下的表，包括视图
    private static final String SHOW_TABLE_AND_VIEW_BY_SCHEMA_SQL = "SELECT table_name FROM information_schema.tables WHERE table_schema = '%s' ";

    // 获取指定schema下的表，不包括视图
    private static final String SHOW_TABLE_BY_SCHEMA_SQL = "SELECT table_name FROM information_schema.tables WHERE table_schema = '%s' AND table_type = 'BASE TABLE' ";

    //获取所有表名，包括视图，表名前拼接schema，并对schema和tableName进行增加双引号处理
    private static final String ALL_TABLE_AND_VIEW_SQL = "SELECT '\"'||table_schema||'\".\"'||table_name||'\"' AS schema_table FROM information_schema.tables order by schema_table ";

    //获取所有表名，不包括视图，表名前拼接schema，并对schema和tableName进行增加双引号处理
    private static final String ALL_TABLE_SQL = "SELECT '\"'||table_schema||'\".\"'||table_name||'\"' AS schema_table FROM information_schema.tables WHERE table_type = 'BASE TABLE'  order by schema_table ";

    // 获取正在使用数据库
    private static final String CURRENT_DB = "select current_database()";

    @Override
    protected ConnFactory getConnFactory() {
        return new PostgresqlConnFactory();
    }

    @Override
    protected DataSourceType getSourceType() {
        return DataSourceType.PostgreSQL;
    }

    @Override
    public List<String> getTableList(ISourceDTO iSource, SqlQueryDTO queryDTO) throws Exception {
        PostgresqlSourceDTO postgresqlSourceDTO = (PostgresqlSourceDTO) iSource;
        Integer clearStatus = beforeQuery(postgresqlSourceDTO, queryDTO, false);
        Statement statement = null;
        ResultSet rs = null;
        try {
            statement = postgresqlSourceDTO.getConnection().createStatement();
            //大小写区分，不传schema默认获取所有表，并且表名签名拼接schema，格式："schema"."tableName"
            String querySql;
            if (StringUtils.isNotBlank(postgresqlSourceDTO.getSchema())) {
                querySql = queryDTO.getView() ? String.format(SHOW_TABLE_AND_VIEW_BY_SCHEMA_SQL, postgresqlSourceDTO.getSchema()) : String.format(SHOW_TABLE_BY_SCHEMA_SQL, postgresqlSourceDTO.getSchema());
            }else {
                querySql = queryDTO.getView() ? ALL_TABLE_AND_VIEW_SQL : ALL_TABLE_SQL;
            }
            rs = statement.executeQuery(querySql);
            List<String> tableList = new ArrayList<>();
            while (rs.next()) {
                tableList.add(rs.getString(1));
            }
            return tableList;
        } catch (Exception e) {
            throw new DtLoaderException("获取表异常", e);
        } finally {
            DBUtil.closeDBResources(rs, statement, postgresqlSourceDTO.clearAfterGetConnection(clearStatus));
        }
    }

    @Override
    public String getTableMetaComment(ISourceDTO iSource, SqlQueryDTO queryDTO) throws Exception {
        PostgresqlSourceDTO postgresqlSourceDTO = (PostgresqlSourceDTO) iSource;
        Integer clearStatus = beforeColumnQuery(postgresqlSourceDTO, queryDTO);

        Statement statement = null;
        ResultSet resultSet = null;
        try {
            statement = postgresqlSourceDTO.getConnection().createStatement();
            resultSet = statement.executeQuery(String.format("select relname as tabname,\n" +
                    "cast(obj_description(relfilenode,'pg_class') as varchar) as comment from pg_class c\n" +
                    "where  relkind = 'r' and relname = '%s'", queryDTO.getTableName()));
            while (resultSet.next()) {
                String dbTableName = resultSet.getString(1);
                if (dbTableName.equalsIgnoreCase(queryDTO.getTableName())) {
                    return resultSet.getString(2);
                }
            }
        } catch (Exception e) {
            throw new DtLoaderException(String.format("获取表:%s 的信息时失败. 请联系 DBA 核查该库、表信息.",
                    queryDTO.getTableName()), e);
        } finally {
            DBUtil.closeDBResources(resultSet, statement, postgresqlSourceDTO.clearAfterGetConnection(clearStatus));
        }
        return "";
    }

    @Override
    protected String doDealType(ResultSetMetaData rsMetaData, Integer los) throws SQLException {
        String type = super.doDealType(rsMetaData, los);

        // bigserial 需要转换
        if (BIGSERIAL.equalsIgnoreCase(type)) {
            return "int8";
        }

        return type;
    }

    @Override
    public IDownloader getDownloader(ISourceDTO source, SqlQueryDTO queryDTO) throws Exception {
        PostgresqlSourceDTO postgresqlSourceDTO = (PostgresqlSourceDTO) source;
        PostgresqlDownloader postgresqlDownloader = new PostgresqlDownloader(getCon(postgresqlSourceDTO), queryDTO.getSql(), postgresqlSourceDTO.getSchema());
        postgresqlDownloader.configure();
        return postgresqlDownloader;
    }

    @Override
    public List<ColumnMetaDTO> getFlinkColumnMetaData(ISourceDTO source, SqlQueryDTO queryDTO) throws Exception {
        Integer clearStatus = beforeColumnQuery(source, queryDTO);
        PostgresqlSourceDTO postgresqlSourceDTO = (PostgresqlSourceDTO) source;
        Statement statement = null;
        ResultSet rs = null;
        List<ColumnMetaDTO> columns = new ArrayList<>();
        try {
            statement = postgresqlSourceDTO.getConnection().createStatement();
            String queryColumnSql = "select * from " + transferSchemaAndTableName(postgresqlSourceDTO.getSchema(), queryDTO.getTableName())
                    + " where 1=2";
            rs = statement.executeQuery(queryColumnSql);
            ResultSetMetaData rsMetaData = rs.getMetaData();
            for (int i = 0, len = rsMetaData.getColumnCount(); i < len; i++) {
                ColumnMetaDTO columnMetaDTO = new ColumnMetaDTO();
                columnMetaDTO.setKey(rsMetaData.getColumnName(i + 1));
                String type = rsMetaData.getColumnTypeName(i + 1);
                int columnType = rsMetaData.getColumnType(i + 1);
                int precision = rsMetaData.getPrecision(i + 1);
                int scale = rsMetaData.getScale(i + 1);
                //postgresql类型转换
                String flinkSqlType = PostgreSqlAdapter.mapColumnTypeJdbc2Java(columnType, precision, scale);
                if (StringUtils.isNotEmpty(flinkSqlType)) {
                    type = flinkSqlType;
                }
                columnMetaDTO.setType(type);
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
                throw new DtLoaderException(String.format("获取表:%s 的字段的元信息时失败. 请联系 DBA 核查该库、表信息.", queryDTO.getTableName()), e);
            }
        } finally {
            DBUtil.closeDBResources(rs, statement, postgresqlSourceDTO.clearAfterGetConnection(clearStatus));
        }

    }

    @Override
    public String getCreateTableSql(ISourceDTO source, SqlQueryDTO queryDTO) throws Exception {
        throw new DtLoaderException("Not Support");
    }

    @Override
    public List<ColumnMetaDTO> getPartitionColumn(ISourceDTO source, SqlQueryDTO queryDTO) throws Exception {
        throw new DtLoaderException("Not Support");
    }

    @Override
    public String getShowDbSql() {
        return DATABASE_QUERY;
    }

    protected String getTableBySchemaSql(ISourceDTO sourceDTO, SqlQueryDTO queryDTO) {
        return String.format(SHOW_TABLE_BY_SCHEMA_SQL, queryDTO.getSchema());
    }

    /**
     * 处理Postgresql schema和tableName，适配schema和tableName中有.的情况
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

    /**
     * 处理Postgresql数据预览sql
     * @param iSourceDTO
     * @param sqlQueryDTO
     * @return
     */
    @Override
    protected String dealSql(ISourceDTO iSourceDTO, SqlQueryDTO sqlQueryDTO){
        PostgresqlSourceDTO postgresqlSourceDTO = (PostgresqlSourceDTO)iSourceDTO;
        return "select * from " + transferSchemaAndTableName(postgresqlSourceDTO.getSchema(), sqlQueryDTO.getTableName())
                + " limit " + sqlQueryDTO.getPreviewNum();
    }

    @Override
    protected String getCurrentDbSql() {
        return CURRENT_DB;
    }
}
