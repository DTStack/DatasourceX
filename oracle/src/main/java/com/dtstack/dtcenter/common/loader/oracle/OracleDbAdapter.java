package com.dtstack.dtcenter.common.loader.oracle;

import java.sql.Types;

/**
 * @Author: 尘二(chener @ dtstack.com)
 * @Date: 2019/4/3 17:03
 * @Description:
 */
public class OracleDbAdapter {
    public static String mapColumnTypeJdbc2Oracle(final int columnType, int precision, int scale) {
        //TODO 转化成用户读(oracle显示)类型
        return null;
    }

    public static String mapColumnTypeJdbc2Java(final int columnType, int precision, int scale) {
        switch (columnType) {
            case Types.CHAR:
            case Types.CLOB:
            case Types.LONGVARCHAR:
            case Types.VARCHAR:
            case Types.NVARCHAR:
            case Types.NCLOB:
                return JavaType.TYPE_VARCHAR.getFlinkSqlType();

            case Types.DATE:
            case Types.TIME:
            case Types.TIMESTAMP:
                return JavaType.TYPE_TIMESTAMP.getFlinkSqlType();


            case Types.BIGINT:
                return JavaType.TYPE_BIGINT.getFlinkSqlType();
            case Types.INTEGER:
            case Types.SMALLINT:
            case Types.TINYINT:
                return JavaType.TYPE_INT.getFlinkSqlType();


            case Types.BIT:
                return JavaType.TYPE_BOOLEAN.getFlinkSqlType();

            case Types.DECIMAL:
            case Types.NUMERIC:
                return JavaType.TYPE_DECIMAL.getFlinkSqlType();
            case Types.DOUBLE:
            case Types.FLOAT:
                return JavaType.TYPE_DOUBLE.getFlinkSqlType();
            case Types.REAL:
                return JavaType.TYPE_FLOAT.getFlinkSqlType();

            default:
                return null;
        }
    }

    public enum JavaType {
        TYPE_BOOLEAN("boolean"),
        TYPE_INT("int"),
        TYPE_INTEGER("integer"),
        TYPE_BIGINT("bigint"),
        TYPE_TINYINT("tinyint"),
        TYPE_SMALLINT("smallint"),
        TYPE_VARCHAR("varchar"),
        TYPE_FLOAT("float"),
        TYPE_DOUBLE("double"),
        TYPE_DATE("date"),
        TYPE_TIMESTAMP("timestamp"),
        TYPE_DECIMAL("decimal");

        private String flinkSqlType;

        JavaType(String flinkSqlType) {
            this.flinkSqlType = flinkSqlType;
        }

        public String getFlinkSqlType() {
            return flinkSqlType;
        }
    }
}