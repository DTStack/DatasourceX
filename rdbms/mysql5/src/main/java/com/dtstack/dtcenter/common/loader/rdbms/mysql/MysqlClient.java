package com.dtstack.dtcenter.common.loader.rdbms.mysql;

import com.dtstack.dtcenter.common.enums.DataSourceType;
import com.dtstack.dtcenter.common.exception.DBErrorCode;
import com.dtstack.dtcenter.common.exception.DtCenterDefException;
import com.dtstack.dtcenter.common.loader.rdbms.common.AbsRdbmsClient;
import com.dtstack.dtcenter.common.loader.rdbms.common.ConnFactory;
import com.dtstack.dtcenter.loader.DtClassConsistent;
import com.dtstack.dtcenter.loader.dto.SourceDTO;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.utils.DBUtil;

import java.sql.*;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 17:18 2020/1/3
 * @Description：Mysql 客户端
 */
public class MysqlClient extends AbsRdbmsClient {
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
        if (tableName.contains(".")) {
            String[] tables = tableName.split("\\.");
            tableName = tables[1];
            return String.format("%s.%s", tables[0], tableName.contains("`") ? tableName : String.format("`%s`",
                    tableName));
        }
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
    public String getTableMetaComment(SourceDTO source, SqlQueryDTO queryDTO) throws Exception {
        Integer clearStatus = beforeColumnQuery(source, queryDTO);
        Statement statement = null;
        ResultSet resultSet = null;

        try {
            statement = source.getConnection().createStatement();
            resultSet = statement.executeQuery("show table status");
            while (resultSet.next()) {
                String dbTableName = resultSet.getString(1);

                if (dbTableName.equalsIgnoreCase(queryDTO.getTableName())) {
                    return resultSet.getString(DtClassConsistent.PublicConsistent.COMMENT);
                }
            }
        } catch (Exception e) {
            throw new DtCenterDefException(String.format("获取表:%s 的信息时失败. 请联系 DBA 核查该库、表信息.",
                    queryDTO.getTableName()),
                    DBErrorCode.GET_COLUMN_INFO_FAILED, e);
        } finally {
            DBUtil.closeDBResources(resultSet, statement, source.clearAfterGetConnection(clearStatus));
        }
        return null;
    }
}
