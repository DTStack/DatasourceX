package com.dtstack.dtcenter.common.loader.common.utils;

import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.source.RdbmsSourceDTO;
import com.dtstack.dtcenter.loader.enums.ConnectionClearStatus;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 11:13 2020/1/13
 * @Description：数据库工具类
 */
@Slf4j
public class DBUtil {

    // 默认最大查询条数
    private static final Integer MAX_QUERY_ROW = 5000;

    // 字段重复时的重命名规则
    private static final String REPEAT_SIGN = "%s(%s)";

    /**
     * 根据 SQL 查询
     *
     * @param conn
     * @param sql
     * @return
     */
    public static List<Map<String, Object>> executeQuery(Connection conn, String sql) {
        return executeQuery(conn, sql, MAX_QUERY_ROW, null);
    }

    /**
     * 根据 SQL 查询
     *
     * @param conn
     * @param sql
     * @param limit
     * @param queryTimeout
     * @return
     */
    public static List<Map<String, Object>> executeQuery(Connection conn, String sql, Integer limit, Integer queryTimeout) {
        List<Map<String, Object>> result = Lists.newArrayList();
        ResultSet res = null;
        Statement statement = null;
        try {
            statement = conn.createStatement();
            if (queryTimeout != null) {
                try {
                    statement.setQueryTimeout(queryTimeout);
                } catch (Exception e) {
                    log.debug(String.format("statement set QueryTimeout exception,%s", e.getMessage()), e);
                }
            }
            // 设置返回最大条数
            statement.setMaxRows(Objects.isNull(limit) ? MAX_QUERY_ROW : limit);

            if (statement.execute(sql)) {
                res = statement.getResultSet();
                int columns = res.getMetaData().getColumnCount();
                List<String> columnName = Lists.newArrayList();
                for (int i = 0; i < columns; i++) {
                    columnName.add(res.getMetaData().getColumnLabel(i + 1));
                }

                while (res.next()) {
                    Map<String, Object> row = Maps.newLinkedHashMap();
                    Map<String, Integer> columnRepeatSign = Maps.newHashMap();
                    for (int i = 0; i < columns; i++) {
                        String column = dealRepeatColumn(row, columnName.get(i), columnRepeatSign);
                        row.put(column, res.getObject(i + 1));
                    }
                    result.add(row);
                }
            }

        } catch (Exception e) {
            throw new DtLoaderException(String.format("SQL execute exception：%s", e.getMessage()), e);
        } finally {
            DBUtil.closeDBResources(res, statement, null);
        }
        return result;
    }

    /**
     * 根据 SQL 查询 - 预编译查询
     *
     * @param conn
     * @param sql
     * @param limit
     * @param preFields
     * @param queryTimeout
     * @return
     */
    public static List<Map<String, Object>> executeQuery(Connection conn, String sql, Integer limit, List<Object> preFields, Integer queryTimeout) {
        List<Map<String, Object>> result = Lists.newArrayList();
        ResultSet res = null;
        PreparedStatement statement = null;
        try {
            statement = conn.prepareStatement(sql);
            //设置查询超时时间
            if (queryTimeout != null) {
                try {
                    statement.setQueryTimeout(queryTimeout);
                } catch (Exception e) {
                    log.debug(String.format("statement set QueryTimeout exception,%s", e.getMessage()), e);
                }
            }
            // 设置返回最大条数
            statement.setMaxRows(Objects.isNull(limit) ? MAX_QUERY_ROW : limit);

            //todo 支持预编译sql
            if (preFields != null && !preFields.isEmpty()) {
                for (int i = 0; i < preFields.size(); i++) {
                    statement.setObject(i + 1, preFields.get(i));
                }
            }
            res = statement.executeQuery();
            int columns = res.getMetaData().getColumnCount();
            List<String> columnName = Lists.newArrayList();
            for (int i = 0; i < columns; i++) {
                columnName.add(res.getMetaData().getColumnLabel(i + 1));
            }

            while (res.next()) {
                Map<String, Object> row = Maps.newLinkedHashMap();
                Map<String, Integer> columnRepeatSign = Maps.newHashMap();
                for (int i = 0; i < columns; i++) {
                    String column = dealRepeatColumn(row, columnName.get(i), columnRepeatSign);
                    row.put(column, res.getObject(i + 1));
                }
                result.add(row);
            }
        } catch (Exception e) {
            throw new DtLoaderException(String.format("SQL executed exception, %s", e.getMessage()), e);
        } finally {
            DBUtil.closeDBResources(res, statement, null);
        }
        return result;
    }

    /**
     * 处理 executeQuery 查询结果字段重复字段
     *
     * @param row              当前行的数据
     * @param column           当前查询字段名
     * @param columnRepeatSign 当前字段重复次数
     * @return 处理后的重复字段名
     */
    public static String dealRepeatColumn(Map<String, Object> row, String column, Map<String, Integer> columnRepeatSign) {
        boolean repeat = row.containsKey(column);
        if (repeat) {
            // 如果 column 重复则在 column 后进行增加 (1),(2)... 区分处理
            boolean contains = columnRepeatSign.containsKey(column);
            if (!contains) {
                columnRepeatSign.put(column, 1);
            } else {
                columnRepeatSign.put(column, columnRepeatSign.get(column) + 1);
            }
            return String.format(REPEAT_SIGN, column, columnRepeatSign.get(column));
        } else {
            return column;
        }
    }

    /**
     * 执行查询，无需结果集
     *
     * @param conn
     * @param sql
     */
    public static void executeSqlWithoutResultSet(Connection conn, String sql) {
        Statement statement = null;
        try {
            statement = conn.createStatement();
            statement.execute(sql);
        } catch (Exception e) {
            throw new DtLoaderException(String.format("execute sql: %s, cause by：%s", sql, e.getMessage()), e);
        } finally {
            DBUtil.closeDBResources(null, statement, null);
        }
    }

    /**
     * 重置表类型
     * {@link java.sql.DatabaseMetaData#getTableTypes()}
     *
     * @param queryDTO
     * @return
     */
    public static String[] getTableTypes(SqlQueryDTO queryDTO) {
        if (ArrayUtils.isNotEmpty(queryDTO.getTableTypes())) {
            return queryDTO.getTableTypes();
        }

        String[] types = new String[BooleanUtils.isTrue(queryDTO.getView()) ? 2 : 1];
        types[0] = "TABLE";
        if (BooleanUtils.isTrue(queryDTO.getView())) {
            types[1] = "VIEW";
        }
        return types;
    }

    /**
     * 关闭数据库资源信息
     *
     * @param rs
     * @param stmt
     * @param conn
     */
    public static void closeDBResources(ResultSet rs, Statement stmt, Connection conn) {
        if (null != rs) {
            try {
                rs.close();
            } catch (SQLException e) {
                log.error(e.getMessage(), e);
            }
        }


        if (null != stmt) {
            try {
                stmt.close();
            } catch (SQLException e) {
                log.error(e.getMessage(), e);
            }
        }

        if (null != conn) {
            try {
                conn.close();
            } catch (SQLException e) {
                log.error(e.getMessage(), e);
            }
        }
    }

    /**
     * 环境变量转化
     *
     * @param taskParams
     * @return
     */
    public static Properties stringToProperties (String taskParams) {
        Properties properties = new Properties();

        // 空指针判断
        if (StringUtils.isBlank(taskParams)) {
            return properties;
        }

        try {
            properties.load(new ByteArrayInputStream(taskParams.replace("hiveconf:", "").getBytes("UTF-8")));
        } catch (IOException e) {
            log.error("taskParams change error : {}", e.getMessage(), e);
        }
        return properties;
    }

    /**
     * 处理RdbmsSourceDTO对象里面的Connection属性
     *
     * @param sourceDTO
     * @param clearStatus
     * @return
     */
    public static Connection clearAfterGetConnection(RdbmsSourceDTO sourceDTO, Integer clearStatus) {
        if (ConnectionClearStatus.NORMAL.getValue().equals(clearStatus)) {
            return null;
        }

        Connection temp = sourceDTO.getConnection();
        sourceDTO.setConnection(null);

        if (ConnectionClearStatus.CLEAR.getValue().equals(clearStatus)) {
            return null;
        }
        return temp;
    }


    /**
     * JDBC 每次读取数据的行数
     */
    public static void setFetchSize(Statement statement, SqlQueryDTO sqlQueryDTO) {

        if (ReflectUtil.fieldExists(SqlQueryDTO.class, "fetchSize")) {
            return;
        }
        Integer fetchSize = sqlQueryDTO.getFetchSize();
        setFetchSize(statement, fetchSize);
    }

    public static void setFetchSize(Statement statement, Integer fetchSize) {
        try {
            if (fetchSize != null && fetchSize > 0) {
                statement.setFetchSize(fetchSize);
            }
        } catch (Exception e) {
            log.error("set fetchSize error,{}", e.getMessage());
        }
    }
}
