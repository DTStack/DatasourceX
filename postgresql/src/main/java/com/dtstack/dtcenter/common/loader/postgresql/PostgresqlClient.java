package com.dtstack.dtcenter.common.loader.postgresql;

import com.dtstack.dtcenter.common.exception.DBErrorCode;
import com.dtstack.dtcenter.common.exception.DtCenterDefException;
import com.dtstack.dtcenter.common.loader.common.AbsRdbmsClient;
import com.dtstack.dtcenter.common.loader.common.ConnFactory;
import com.dtstack.dtcenter.loader.IDownloader;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;
import com.dtstack.dtcenter.loader.dto.source.PostgresqlSourceDTO;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import com.dtstack.dtcenter.loader.utils.DBUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 15:52 2020/1/7
 * @Description：Postgresql 客户端
 */
@Slf4j
public class PostgresqlClient extends AbsRdbmsClient {
    private static final String BIGSERIAL = "bigserial";

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

        String database = getPostgreSchema(postgresqlSourceDTO.getConnection(), postgresqlSourceDTO.getUsername());
        if (StringUtils.isNotBlank(database) && database.contains(",")) {
            //处理 "root,'public"这种情况
            String[] split = database.split(",");
            if (split.length > 1 && StringUtils.isNotBlank(split[1])) {
                database = split[1].replace("'", "").trim();
            }
        }

        Statement statement = null;
        ResultSet rs = null;
        try {
            statement = postgresqlSourceDTO.getConnection().createStatement();
            //大小写区分
            rs = statement.executeQuery(String.format("select table_name from information_schema.tables WHERE " +
                    "table_schema in ( '%s' )", database));
            List<String> tableList = new ArrayList<>();
            while (rs.next()) {
                tableList.add(rs.getString(1));
            }
            return tableList;
        } catch (Exception e) {
            throw new DtCenterDefException("获取表异常", e);
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
            throw new DtCenterDefException(String.format("获取表:%s 的信息时失败. 请联系 DBA 核查该库、表信息.",
                    queryDTO.getTableName()),
                    DBErrorCode.GET_COLUMN_INFO_FAILED, e);
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

    /**
     * 获取 SCHEMA
     *
     * @param conn
     * @param user
     * @return
     * @throws SQLException
     */
    private static String getPostgreSchema(Connection conn, String user) throws SQLException {
        ResultSet rs = null;
        Statement statement = null;
        List<String> tableList = new ArrayList<>();
        try {
            statement = conn.createStatement();
            rs = statement.executeQuery("SHOW search_path;");
            while (rs.next()) {
                tableList.add(rs.getString("search_path"));
            }

        } catch (SQLException e) {
            throw e;
        } finally {
            DBUtil.closeDBResources(rs, statement, null);
        }
        Set<String> backList = new HashSet<>();
        for (String table : tableList) {
            if (!table.contains(",")) {
                backList.add("\"$user\"".equals(table) ? user : table.trim());
            }
            String[] tables = table.split(",");
            for (String temptable : tables) {
                backList.add("\"$user\"".equals(temptable) ? user : temptable.trim());
            }
        }
        return String.join("','", backList);
    }

    @Override
    public IDownloader getDownloader(ISourceDTO source, SqlQueryDTO queryDTO) throws Exception {
        PostgresqlSourceDTO postgresqlSourceDTO = (PostgresqlSourceDTO) source;
        PostgresqlDownloader postgresqlDownloader = new PostgresqlDownloader(getCon(postgresqlSourceDTO), queryDTO.getSql(), postgresqlSourceDTO.getSchema());
        postgresqlDownloader.configure();
        return postgresqlDownloader;
    }
}