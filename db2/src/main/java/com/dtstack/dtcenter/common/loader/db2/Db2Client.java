package com.dtstack.dtcenter.common.loader.db2;

import com.dtstack.dtcenter.common.loader.common.utils.DBUtil;
import com.dtstack.dtcenter.common.loader.rdbms.AbsRdbmsClient;
import com.dtstack.dtcenter.common.loader.rdbms.ConnFactory;
import com.dtstack.dtcenter.loader.IDownloader;
import com.dtstack.dtcenter.loader.dto.ColumnMetaDTO;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.source.Db2SourceDTO;
import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;
import com.dtstack.dtcenter.loader.dto.source.RdbmsSourceDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 16:09 2020/1/7
 * @Description：Db2 客户端
 */
@Slf4j
public class Db2Client extends AbsRdbmsClient {
    private static final String TABLE_QUERY = "select tabname from syscat.tables where tabschema = '%s'";

    private static final String DATABASE_QUERY = "select schemaname from syscat.schemata where ownertype != 'S'";

    private static final String TABLE_BY_SCHEMA = "select TABLE_NAME AS Name from SYSIBM.TABLES where TABLE_SCHEMA='%s' %s";

    // 获取db2的当前database
    private static final String CURRENT_DB = "select CURRENT schema from sysibm.sysdummy1";

    // 根据schema选表表名模糊查询
    private static final String SEARCH_SQL = " AND TABLE_NAME LIKE '%s' ";

    // 限制条数语句
    private static final String LIMIT_SQL = " fetch first  %s rows only ";

    // 获取当前版本号
    private static final String SHOW_VERSION = "SELECT SERVICE_LEVEL FROM SYSIBMADM.ENV_INST_INFO";

    @Override
    protected ConnFactory getConnFactory() {
        return new Db2ConnFactory();
    }

    @Override
    protected DataSourceType getSourceType() {
        return DataSourceType.DB2;
    }

    @Override
    public List<String> getTableList(ISourceDTO iSource, SqlQueryDTO queryDTO) {
        Integer clearStatus = beforeQuery(iSource, queryDTO, false);
        Db2SourceDTO db2SourceDTO = (Db2SourceDTO) iSource;
        Statement statement = null;
        ResultSet rs = null;
        List<String> tableList = new ArrayList<>();
        try {
            String sql = String.format(TABLE_QUERY, db2SourceDTO.getUsername().toUpperCase());
            statement = db2SourceDTO.getConnection().createStatement();
            rs = statement.executeQuery(sql);
            int columnSize = rs.getMetaData().getColumnCount();
            while (rs.next()) {
                tableList.add(rs.getString(1));
            }
        } catch (Exception e) {
            throw new DtLoaderException(String.format("get table exception：%s", e.getMessage()), e);
        } finally {
            DBUtil.closeDBResources(rs, statement, DBUtil.clearAfterGetConnection(db2SourceDTO, clearStatus));
        }
        return tableList;
    }

    @Override
    public String getTableMetaComment(ISourceDTO iSource, SqlQueryDTO queryDTO) {
        Integer clearStatus = beforeColumnQuery(iSource, queryDTO);
        Db2SourceDTO db2SourceDTO = (Db2SourceDTO) iSource;

        Statement statement = null;
        ResultSet resultSet = null;
        try {
            statement = db2SourceDTO.getConnection().createStatement();
            resultSet = statement.executeQuery(String.format("select remarks from syscat.tables where tabname = '%s'"
                    , queryDTO.getTableName()));
            while (resultSet.next()) {
                return resultSet.getString("REMARKS");
            }
        } catch (Exception e) {
            throw new DtLoaderException(String.format("Failed to get the information of table: %s. Please contact DBA to check the database and table information: %s",
                    queryDTO.getTableName(), e.getMessage()), e);
        } finally {
            DBUtil.closeDBResources(resultSet, statement, DBUtil.clearAfterGetConnection(db2SourceDTO, clearStatus));
        }
        return "";
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
    public IDownloader getDownloader(ISourceDTO source, SqlQueryDTO queryDTO) throws Exception {
        Db2SourceDTO db2SourceDTO = (Db2SourceDTO) source;
        Connection connection = getCon(source);
        String sql = queryDTO.getSql();
        String schema = db2SourceDTO.getSchema();
        Db2Downloader db2Downloader = new Db2Downloader(connection, sql, schema);
        db2Downloader.configure();
        return db2Downloader;
    }

    @Override
    public String getShowDbSql() {
        return DATABASE_QUERY;
    }

    @Override
    protected String getTableBySchemaSql(ISourceDTO sourceDTO, SqlQueryDTO queryDTO) {
        RdbmsSourceDTO rdbmsSourceDTO = (RdbmsSourceDTO) sourceDTO;
        String schema = StringUtils.isNotBlank(queryDTO.getSchema()) ? queryDTO.getSchema() : rdbmsSourceDTO.getSchema();
        // 如果不传scheme，默认使用当前连接使用的schema
        if (StringUtils.isBlank(schema)) {
            log.info("schema is empty，get current used schema!");
            // 获取当前数据库
            try {
                schema = getCurrentDatabase(sourceDTO);
            } catch (Exception e) {
                throw new DtLoaderException(String.format("get current used database error！,%s", e.getMessage()), e);
            }

        }
        log.info("current used schema：{}", schema);
        StringBuilder constr = new StringBuilder();
        if (StringUtils.isNotBlank(queryDTO.getTableNamePattern())) {
            constr.append(String.format(SEARCH_SQL, addPercentSign(queryDTO.getTableNamePattern().trim())));
        }
        if (Objects.nonNull(queryDTO.getLimit())) {
            constr.append(String.format(LIMIT_SQL, queryDTO.getLimit()));
        }
        return String.format(TABLE_BY_SCHEMA, schema, constr.toString());
    }

    /**
     * 处理db2 schema和tableName，适配schema和tableName中有.的情况
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

    @Override
    protected String getVersionSql() {
        return SHOW_VERSION;
    }
}
