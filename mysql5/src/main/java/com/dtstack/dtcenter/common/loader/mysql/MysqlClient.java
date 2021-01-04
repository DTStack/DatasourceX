package com.dtstack.dtcenter.common.loader.mysql;

import com.dtstack.dtcenter.common.loader.common.DtClassConsistent;
import com.dtstack.dtcenter.common.loader.common.utils.DBUtil;
import com.dtstack.dtcenter.common.loader.rdbms.AbsRdbmsClient;
import com.dtstack.dtcenter.common.loader.rdbms.ConnFactory;
import com.dtstack.dtcenter.loader.IDownloader;
import com.dtstack.dtcenter.loader.dto.ColumnMetaDTO;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;
import com.dtstack.dtcenter.loader.dto.source.Mysql5SourceDTO;
import com.dtstack.dtcenter.loader.dto.source.RdbmsSourceDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 17:18 2020/1/3
 * @Description：Mysql 客户端
 */
@Slf4j
public class MysqlClient extends AbsRdbmsClient {

    // 获取正在使用数据库
    private static final String CURRENT_DB = "select database()";

    // 模糊查询数据库
    private static final String SHOW_DB_LIKE = "show databases like '%s'";

    private static final String DONT_EXIST = "doesn't exist";

    // 获取指定数据库下的表
    private static final String SHOW_TABLE_BY_SCHEMA_SQL = "select table_name from information_schema.tables where table_schema='%s' and table_type='BASE TABLE' %s";

    // 表名正则匹配模糊查询
    private static final String SEARCH_SQL = " AND table_name REGEXP '%s' ";

    // 限制条数语句
    private static final String LIMIT_SQL = " limit %s ";

    // 创建数据库
    private static final String CREATE_SCHEMA_SQL_TMPL = "create schema if not exists %s ";

    // 判断table是否在schema中
    private static final String TABLE_IS_IN_SCHEMA = "select table_name from information_schema.tables where table_schema='%s' and table_name = '%s'";

    @Override
    protected ConnFactory getConnFactory() {
        return new MysqlConnFactory();
    }

    @Override
    protected DataSourceType getSourceType() {
        return DataSourceType.MySQL;
    }

    @Override
    protected String transferTableName(String tableName) {
        return tableName.contains("`") ? tableName : String.format("`%s`", tableName);
    }

    @Override
    protected String doDealType(ResultSetMetaData rsMetaData, Integer los) throws SQLException {
        int columnType = rsMetaData.getColumnType(los + 1);
        // text,mediumtext,longtext的jdbc类型名都是varchar，需要区分。不同的编码下，最大存储长度也不同。考虑1，2，3，4字节的编码

        if (columnType != Types.LONGVARCHAR) {
            return super.doDealType(rsMetaData, los);
        }

        int precision = rsMetaData.getPrecision(los + 1);
        if (precision >= 16383 & precision <= 65535) {
            return "TEXT";
        }

        if (precision >= 4194303 & precision <= 16777215) {
            return "MEDIUMTEXT";
        }

        if (precision >= 536870911 & precision <= 2147483647) {
            return "LONGTEXT";
        }

        return super.doDealType(rsMetaData, los);
    }

    @Override
    public String getTableMetaComment(ISourceDTO iSource, SqlQueryDTO queryDTO) {
        Mysql5SourceDTO mysql5SourceDTO = (Mysql5SourceDTO) iSource;
        Integer clearStatus = beforeColumnQuery(mysql5SourceDTO, queryDTO);
        Statement statement = null;
        ResultSet resultSet = null;

        try {
            statement = mysql5SourceDTO.getConnection().createStatement();
            resultSet = statement.executeQuery("show table status");
            while (resultSet.next()) {
                String dbTableName = resultSet.getString(1);

                if (dbTableName.equalsIgnoreCase(queryDTO.getTableName())) {
                    return resultSet.getString(DtClassConsistent.PublicConsistent.COMMENT);
                }
            }
        } catch (Exception e) {
            throw new DtLoaderException(String.format("获取表:%s 的信息时失败. 请联系 DBA 核查该库、表信息.",
                    queryDTO.getTableName()), e);
        } finally {
            DBUtil.closeDBResources(resultSet, statement, mysql5SourceDTO.clearAfterGetConnection(clearStatus));
        }
        return "";
    }

    @Override
    public IDownloader getDownloader(ISourceDTO source, SqlQueryDTO queryDTO) throws Exception {
        Mysql5SourceDTO mysql5SourceDTO = (Mysql5SourceDTO) source;
        MysqlDownloader mysqlDownloader = new MysqlDownloader(getCon(source), queryDTO.getSql(), mysql5SourceDTO.getSchema());
        mysqlDownloader.configure();
        return mysqlDownloader;
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
            String queryColumnCommentSql =
                    "show full columns from " + transferTableName(queryDTO.getTableName());
            rs = statement.executeQuery(queryColumnCommentSql);
            while (rs.next()) {
                String columnName = rs.getString("Field");
                String columnComment = rs.getString("Comment");
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

    @Override
    protected String getCurrentDbSql() {
        return CURRENT_DB;
    }

    @Override
    protected String getCreateDatabaseSql(String dbName, String comment) {
        return String.format(CREATE_SCHEMA_SQL_TMPL, dbName);
    }

    @Override
    public Boolean isDatabaseExists(ISourceDTO source, String dbName) {
        if (StringUtils.isBlank(dbName)) {
            throw new DtLoaderException("数据库名称不能为空");
        }
        return CollectionUtils.isNotEmpty(executeQuery(source, SqlQueryDTO.builder().sql(String.format(SHOW_DB_LIKE, dbName)).build()));
    }

    @Override
    public Boolean isTableExistsInDatabase(ISourceDTO source, String tableName, String dbName) {
        if (StringUtils.isBlank(dbName)) {
            throw new DtLoaderException("数据库名称不能为空");
        }
        return CollectionUtils.isNotEmpty(executeQuery(source, SqlQueryDTO.builder().sql(String.format(TABLE_IS_IN_SCHEMA, dbName, tableName)).build()));
    }

    /**
     * 获取指定schema下的表，如果没有填schema，默认使用当前schema。支持正则匹配查询、条数限制
     * @param sourceDTO 数据源信息
     * @param queryDTO 查询条件
     * @return
     */
    @Override
    protected String getTableBySchemaSql(ISourceDTO sourceDTO, SqlQueryDTO queryDTO) {
        String schema = queryDTO.getSchema();
        // 如果不传scheme，默认使用当前连接使用的schema
        if (StringUtils.isBlank(schema)) {
            log.info("schema为空，获取当前正在使用的schema!");
            // 获取当前数据库
            try {
                schema = getCurrentDatabase(sourceDTO);
            } catch (Exception e) {
                throw new DtLoaderException("获取当前使用数据库失败！", e);
            }

        }
        log.info("当前使用schema：{}", schema);
        StringBuilder constr = new StringBuilder();
        if (StringUtils.isNotBlank(queryDTO.getTableNamePattern())) {
            constr.append(String.format(SEARCH_SQL, queryDTO.getTableNamePattern()));
        }
        if (Objects.nonNull(queryDTO.getLimit())) {
            constr.append(String.format(LIMIT_SQL, queryDTO.getLimit()));
        }
        return String.format(SHOW_TABLE_BY_SCHEMA_SQL, schema, constr.toString());
    }
}
