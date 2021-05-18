package com.dtstack.dtcenter.common.loader.common.utils;

import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
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

    /**
     * 根据 SQL 查询
     *
     * @param conn
     * @param sql
     * @param closeConn 是否关闭连接
     * @return
     */
    public static List<Map<String, Object>> executeQuery(Connection conn, String sql, Boolean closeConn) {
        return executeQuery(conn, sql, null, null, closeConn);
    }

    /**
     * 根据 SQL 查询
     *
     * @param conn
     * @param sql
     * @param limit
     * @param queryTimeout
     * @param closeConn
     * @return
     */
    public static List<Map<String, Object>> executeQuery(Connection conn, String sql, Integer limit, Integer queryTimeout, Boolean closeConn) {
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
                    for (int i = 0; i < columns; i++) {
                        row.put(columnName.get(i), res.getObject(i + 1));
                    }
                    result.add(row);
                }
            }

        } catch (Exception e) {
            throw new DtLoaderException(String.format("SQL execute exception：%s", e.getMessage()), e);
        } finally {
            DBUtil.closeDBResources(res, statement, closeConn ? conn : null);
        }
        return result;
    }

    /**
     * 根据 SQL 查询 - 预编译查询
     *
     * @param conn
     * @param sql
     * @param closeConn 是否关闭连接
     * @return
     */
    public static List<Map<String, Object>> executeQuery(Connection conn, String sql, Boolean closeConn, List<Object> preFields, Integer queryTimeout) {
        return executeQuery(conn, sql, null, closeConn, preFields, queryTimeout);
    }

    /**
     * 根据 SQL 查询 - 预编译查询
     *
     * @param conn
     * @param sql
     * @param limit
     * @param closeConn
     * @param preFields
     * @param queryTimeout
     * @return
     */
    public static List<Map<String, Object>> executeQuery(Connection conn, String sql, Integer limit, Boolean closeConn, List<Object> preFields, Integer queryTimeout) {
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
                for (int i = 0; i < columns; i++) {
                    row.put(columnName.get(i), res.getObject(i + 1));
                }
                result.add(row);
            }
        } catch (Exception e) {
            throw new DtLoaderException(String.format("SQL executed exception, %s", e.getMessage()), e);
        } finally {
            DBUtil.closeDBResources(res, statement, closeConn ? conn : null);
        }
        return result;
    }

    /**
     * 执行查询，无需结果集
     *
     * @param conn
     * @param sql
     * @param closeConn 是否关闭连接
     * @return
     * @throws Exception
     */
    public static void executeSqlWithoutResultSet(Connection conn, String sql, Boolean closeConn) {
        Statement statement = null;
        try {
            statement = conn.createStatement();
            statement.execute(sql);
        } catch (Exception e) {
            throw new DtLoaderException(String.format("SQL execute exception：%s", e.getMessage()), e);
        } finally {
            DBUtil.closeDBResources(null, statement, closeConn ? conn : null);
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
        try {
            if (null != rs) {
                rs.close();
            }

            if (null != stmt) {
                stmt.close();
            }

            if (null != conn) {
                conn.close();
            }
        } catch (Throwable e) {
            log.error(e.getMessage(), e);
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
}
