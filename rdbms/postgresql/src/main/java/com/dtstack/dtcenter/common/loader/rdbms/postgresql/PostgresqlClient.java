package com.dtstack.dtcenter.common.loader.rdbms.postgresql;

import com.dtstack.dtcenter.common.exception.DtCenterDefException;
import com.dtstack.dtcenter.common.loader.rdbms.common.AbsRdbmsClient;
import com.dtstack.dtcenter.common.loader.rdbms.common.ConnFactory;
import com.dtstack.dtcenter.loader.DtClassConsistent;
import com.dtstack.dtcenter.loader.dto.SourceDTO;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.utils.DBUtil;
import org.apache.commons.lang.StringUtils;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 15:52 2020/1/7
 * @Description：Postgresql 客户端
 */
public class PostgresqlClient extends AbsRdbmsClient {
    @Override
    protected ConnFactory getConnFactory() {
        return new PostgresqlCoonFactory();
    }

    @Override
    public List<String> getTableList(SourceDTO source, SqlQueryDTO queryDTO) throws Exception {
        Boolean closeQuery = beforeQuery(source, queryDTO, false);

        String database = getPostgreSchema(source.getConnection(), source.getUsername(), source.getUrl(),
                "currentSchema");
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
            statement = source.getConnection().createStatement();
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
            DBUtil.closeDBResources(rs, statement, closeQuery ? source.getConnection() : null);
        }
    }

    /**
     * 获取 SCHEMA
     *
     * @param conn
     * @param user
     * @param jdbcUrl
     * @param param
     * @return
     * @throws SQLException
     */
    private static String getPostgreSchema(Connection conn, String user, String jdbcUrl, String param) throws SQLException {
        String schema = getJdbcParam(jdbcUrl, param);
        if (StringUtils.isNotBlank(schema)) {
            return schema.trim();
        }

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

    /**
     * 获取 JDBC DB
     *
     * @param jdbcUrl
     * @param param
     * @return
     */
    private static String getJdbcParam(String jdbcUrl, String param) {
        if (StringUtils.isBlank(jdbcUrl) || StringUtils.isBlank(param)) {
            return null;
        }

        Matcher matcher = DtClassConsistent.PatternConsistent.JDBC_PATTERN.matcher(jdbcUrl);
        if (matcher.find()) {
            String matchValue = null;
            try {
                matchValue = matcher.group(param);
            } finally {
                return matchValue;
            }
        }
        return null;
    }
}
