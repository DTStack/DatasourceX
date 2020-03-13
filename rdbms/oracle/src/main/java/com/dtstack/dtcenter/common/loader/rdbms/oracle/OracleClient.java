package com.dtstack.dtcenter.common.loader.rdbms.oracle;

import com.dtstack.dtcenter.common.enums.DataSourceType;
import com.dtstack.dtcenter.common.exception.DBErrorCode;
import com.dtstack.dtcenter.common.exception.DtCenterDefException;
import com.dtstack.dtcenter.common.loader.rdbms.common.AbsRdbmsClient;
import com.dtstack.dtcenter.common.loader.rdbms.common.ConnFactory;
import com.dtstack.dtcenter.loader.DtClassConsistent;
import com.dtstack.dtcenter.loader.dto.SourceDTO;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.utils.DBUtil;
import oracle.jdbc.OracleResultSetMetaData;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 12:00 2020/1/6
 * @Description：Oracle 客户端
 */
public class OracleClient extends AbsRdbmsClient {
    private static final String ORACLE_ALL_TABLES_SQL = "SELECT TABLE_NAME FROM USER_TABLES UNION SELECT GRANTOR||'" +
            ".'||'\"'||TABLE_NAME||'\"' FROM ALL_TAB_PRIVS WHERE grantee = (SELECT USERNAME FROM user_users WHERE " +
            "ROWNUM = 1) ";
    private static final String ORACLE_WITH_VIEWS_SQL = "UNION SELECT VIEW_NAME FROM USER_VIEWS ";

    private static String ORACLE_NUMBER_TYPE = "NUMBER";
    private static String ORACLE_NUMBER_FORMAT = "NUMBER(%d,%d)";

    @Override
    protected ConnFactory getConnFactory() {
        return new OracleConnFactory();
    }

    @Override
    protected DataSourceType getSourceType() {
        return DataSourceType.Oracle;
    }

    @Override
    public List<String> getTableList(SourceDTO source, SqlQueryDTO queryDTO) throws Exception {
        Boolean closeQuery = beforeQuery(source, queryDTO, false);

        Statement statement = null;
        ResultSet rs = null;
        List<String> tableList = new ArrayList<>();
        try {
            String sql = queryDTO != null && queryDTO.getView() ? ORACLE_ALL_TABLES_SQL + ORACLE_WITH_VIEWS_SQL :
                    ORACLE_ALL_TABLES_SQL;
            statement = source.getConnection().createStatement();
            rs = statement.executeQuery(sql);
            int columnSize = rs.getMetaData().getColumnCount();
            while (rs.next()) {
                tableList.add(rs.getString(1));
            }
        } catch (Exception e) {
            throw new DtCenterDefException("获取表异常", e);
        } finally {
            DBUtil.closeDBResources(rs, statement, source.clearAfterGetConnection(closeQuery));
        }
        return tableList;
    }

    @Override
    public String getTableMetaComment(SourceDTO source, SqlQueryDTO queryDTO) throws Exception {
        Boolean closeQuery = beforeColumnQuery(source, queryDTO);

        String tableName = queryDTO.getTableName();
        if (tableName.contains(".")) {
            tableName = tableName.split("\\.")[1];
        }
        tableName = tableName.replace("\"", "");

        Statement statement = null;
        ResultSet resultSet = null;

        try {
            DatabaseMetaData metaData = source.getConnection().getMetaData();
            resultSet = metaData.getTables(null, null, tableName, null);
            while (resultSet.next()) {
                String comment = resultSet.getString(DtClassConsistent.PublicConsistent.REMARKS);
                return comment;
            }
        } catch (Exception e) {
            throw new DtCenterDefException(String.format("获取表:%s 的信息时失败. 请联系 DBA 核查该库、表信息.",
                    queryDTO.getTableName()),
                    DBErrorCode.GET_COLUMN_INFO_FAILED, e);
        } finally {
            DBUtil.closeDBResources(resultSet, statement, source.clearAfterGetConnection(closeQuery));
        }
        return null;
    }

    @Override
    protected String transferTableName(String tableName) {
        return tableName.contains("\"") ? tableName : String.format("\"%s\"", tableName);
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
}
