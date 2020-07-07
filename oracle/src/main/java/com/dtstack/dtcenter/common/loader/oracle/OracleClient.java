package com.dtstack.dtcenter.common.loader.oracle;

import com.dtstack.dtcenter.common.enums.DataSourceType;
import com.dtstack.dtcenter.common.exception.DBErrorCode;
import com.dtstack.dtcenter.common.exception.DtCenterDefException;
import com.dtstack.dtcenter.common.loader.common.AbsRdbmsClient;
import com.dtstack.dtcenter.common.loader.common.ConnFactory;
import com.dtstack.dtcenter.loader.DtClassConsistent;
import com.dtstack.dtcenter.loader.IDownloader;
import com.dtstack.dtcenter.loader.dto.ColumnMetaDTO;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;
import com.dtstack.dtcenter.loader.dto.source.OracleSourceDTO;
import com.dtstack.dtcenter.loader.utils.CollectionUtil;
import com.dtstack.dtcenter.loader.utils.DBUtil;
import oracle.jdbc.OracleResultSetMetaData;
import org.apache.commons.lang.StringUtils;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
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
    private static final String DONT_EXIST = "doesn't exist";

    @Override
    protected ConnFactory getConnFactory() {
        return new OracleConnFactory();
    }

    @Override
    protected DataSourceType getSourceType() {
        return DataSourceType.Oracle;
    }

    @Override
    public List<String> getTableList(ISourceDTO ISource, SqlQueryDTO queryDTO) throws Exception {
        OracleSourceDTO oracleSourceDTO = (OracleSourceDTO) ISource;
        Integer clearStatus = beforeQuery(oracleSourceDTO, queryDTO, false);

        Statement statement = null;
        ResultSet rs = null;
        List<String> tableList = new ArrayList<>();
        try {
            String sql = queryDTO != null && queryDTO.getView() ? ORACLE_ALL_TABLES_SQL + ORACLE_WITH_VIEWS_SQL :
                    ORACLE_ALL_TABLES_SQL;
            statement = oracleSourceDTO.getConnection().createStatement();
            rs = statement.executeQuery(sql);
            int columnSize = rs.getMetaData().getColumnCount();
            while (rs.next()) {
                tableList.add(rs.getString(1));
            }
        } catch (Exception e) {
            throw new DtCenterDefException("获取表异常", e);
        } finally {
            DBUtil.closeDBResources(rs, statement, oracleSourceDTO.clearAfterGetConnection(clearStatus));
        }
        return tableList;
    }

    @Override
    public String getTableMetaComment(ISourceDTO ISource, SqlQueryDTO queryDTO) throws Exception {
        OracleSourceDTO oracleSourceDTO = (OracleSourceDTO) ISource;
        Integer clearStatus = beforeColumnQuery(oracleSourceDTO, queryDTO);

        String tableName = queryDTO.getTableName();
        if (tableName.contains(".")) {
            tableName = tableName.split("\\.")[1];
        }
        tableName = tableName.replace("\"", "");

        Statement statement = null;
        ResultSet resultSet = null;

        try {
            DatabaseMetaData metaData = oracleSourceDTO.getConnection().getMetaData();
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
            DBUtil.closeDBResources(resultSet, statement, oracleSourceDTO.clearAfterGetConnection(clearStatus));
        }
        return "";
    }

    @Override
    public List<ColumnMetaDTO> getFlinkColumnMetaData(ISourceDTO ISource, SqlQueryDTO queryDTO) throws Exception {
        OracleSourceDTO oracleSourceDTO = (OracleSourceDTO) ISource;
        Integer clearStatus = beforeColumnQuery(oracleSourceDTO, queryDTO);
        Statement statement = null;
        ResultSet rs = null;
        List<ColumnMetaDTO> columns = new ArrayList<>();
        try {
            statement = oracleSourceDTO.getConnection().createStatement();
            String tableName = transferTableName(queryDTO.getTableName());
            String queryColumnSql;
            boolean tableHasTransfer = false;
            String[] split = tableName.split("\\.");
            if(split.length > 1){
                tableHasTransfer = split[1].startsWith("\"");
            }
            if (tableName.contains(".") && !tableHasTransfer) {
                String ku = transferTableName(split[0]);
                tableName = transferTableName(split[1]);
                queryColumnSql = "select " + CollectionUtil.listToStr(queryDTO.getColumns()) + " from " + ku + '"' + "." + '"' + tableName
                        + " where 1=2";
            } else {
                queryColumnSql = "select " + CollectionUtil.listToStr(queryDTO.getColumns()) + " from " + tableName
                        + " where 1=2";
            }

            rs = statement.executeQuery(queryColumnSql);
            ResultSetMetaData rsMetaData = rs.getMetaData();
            for (int i = 0, len = rsMetaData.getColumnCount(); i < len; i++) {
                ColumnMetaDTO columnMetaDTO = new ColumnMetaDTO();
                columnMetaDTO.setKey(rsMetaData.getColumnName(i + 1));
                String flinkSqlType = OracleDbAdapter.mapColumnTypeJdbc2Java(rsMetaData.getColumnType(i + 1), rsMetaData.getPrecision(i + 1), rsMetaData.getScale(i + 1));
                if (StringUtils.isNotEmpty(flinkSqlType)) {
                    columnMetaDTO.setType(flinkSqlType);
                }
                columnMetaDTO.setPart(false);
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
                throw new DtCenterDefException(queryDTO.getTableName() + "表不存在", DBErrorCode.TABLE_NOT_EXISTS, e);
            } else {
                throw new DtCenterDefException(String.format("获取表:%s 的字段的元信息时失败. 请联系 DBA 核查该库、表信息.",
                        queryDTO.getTableName()),
                        DBErrorCode.GET_COLUMN_INFO_FAILED, e);
            }
        } finally {
            DBUtil.closeDBResources(rs, statement, oracleSourceDTO.clearAfterGetConnection(clearStatus));
        }
    }

    @Override
    protected String transferTableName(String tableName) {
        //tableName 可能存在的情况 需要兼容
        //"db"."tableName"
        //db.table
        //table
        if (tableName.contains("\"")) {
            return tableName;
        }

        if (tableName.contains(".")) {
            String[] split = tableName.split("\\.");
            tableName = addQuotes(split[0]) + "." + addQuotes(split[1]);
            return tableName;
        }
        tableName = addQuotes(tableName);
        return tableName;
    }

    private static String addQuotes(String str) {
        str =  str.contains("\"") ? str : String.format("\"%s\"", str);
        return str;
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

    @Override
    public IDownloader getDownloader(ISourceDTO source, SqlQueryDTO queryDTO) throws Exception {
        OracleSourceDTO oracleSourceDTO = (OracleSourceDTO) source;
        OracleDownloader oracleDownloader = new OracleDownloader(getCon(oracleSourceDTO), queryDTO.getSql(), oracleSourceDTO.getSchema());
        oracleDownloader.configure();
        return oracleDownloader;
    }

    @Override
    protected String dealSql(SqlQueryDTO sqlQueryDTO) {
        return "select * from " + transferTableName(sqlQueryDTO.getTableName()) + " where rownum <=" + sqlQueryDTO.getPreviewNum();
    }
}
