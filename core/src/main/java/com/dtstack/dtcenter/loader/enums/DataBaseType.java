package com.dtstack.dtcenter.loader.enums;

import com.dtstack.dtcenter.common.exception.DBErrorCode;
import com.dtstack.dtcenter.common.exception.DtCenterDefException;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 11:13 2020/1/6
 */
public enum DataBaseType {
    MySql("mysql", "com.mysql.jdbc.Driver","select 1111"),
    TDDL("mysql", "com.mysql.jdbc.Driver","select 1111"),
    DRDS("drds", "com.mysql.jdbc.Driver","select 1111"),
    Oracle("oracle", "oracle.jdbc.OracleDriver", "select 1111 from dual"),
    SQLServer("sqlserver", "net.sourceforge.jtds.jdbc.Driver","select 1111"),
    PostgreSQL("postgresql", "org.postgresql.Driver","select 1111"),
    RDBMS("rdbms", "com.alibaba.rdbms.plugin.rdbms.util.DataBaseType","select 1111"),
    DB2("db2", "com.ibm.db2.jcc.DB2Driver","select 1111"),
    HIVE("hive", "org.apache.hive.jdbc.HiveDriver", "show tables"),
    CarbonData("carbonData", "org.apache.hive.jdbc.HiveDriver", "show tables"),
    Spark("Spark", "org.apache.hive.jdbc.HiveDriver", "show tables"),
    ADS("ads","com.mysql.jdbc.Driver","select 1111"),
    RDS("rds","com.mysql.jdbc.Driver","select 1111"),
    MaxCompute("maxcompute","com.aliyun.odps.jdbc.OdpsDriver","select 1111"),
    LIBRA("libra", "org.postgresql.Driver", "show tables"),
    GBase8a("GBase8a","com.gbase.jdbc.Driver", "show tables"),
    Kylin("Kylin","org.apache.kylin.jdbc.Driver", "show tables"),
    Kudu("Kudu","org.apache.hive.jdbc.HiveDriver", "show tables"),
    Impala("Impala", "com.cloudera.impala.jdbc41.Driver", "show tables"),
    Clickhouse("Clickhouse","ru.yandex.clickhouse.ClickHouseDriver", "show tables"),
    HIVE1X("hive", "org.apache.hive.jdbc.HiveDriver", "show tables"),
    Polardb_For_MySQL("polardb for mysql","com.mysql.jdbc.Driver","select 1111");

    private String typeName;
    private String driverClassName;
    private String testSql;

    DataBaseType(String typeName, String driverClassName, String testSql) {
        this.typeName = typeName;
        this.driverClassName = driverClassName;
        this.testSql = testSql;
    }

    public String getDriverClassName() {
        return this.driverClassName;
    }

    public String getTestSql() {
        return testSql;
    }

    public String appendJDBCSuffixForReader(String jdbc) {
        String result = jdbc;
        String suffix = null;
        switch (this) {
            case MySql:
            case DRDS:
                suffix = "yearIsDateType=false&zeroDateTimeBehavior=convertToNull&tinyInt1isBit=false&rewriteBatchedStatements=true";
                if (jdbc.contains("?")) {
                    result = jdbc + "&" + suffix;
                } else {
                    result = jdbc + "?" + suffix;
                }
                break;
            case Oracle:
                break;
            case SQLServer:
                break;
            case DB2:
                break;
            case PostgreSQL:
                break;
            case RDBMS:
                break;
            case LIBRA:
                break;
            default:
                throw new DtCenterDefException("unsupported database type.", DBErrorCode.UNSUPPORTED_TYPE);
        }

        return result;
    }

    public String appendJDBCSuffixForWriter(String jdbc) {
        String result = jdbc;
        String suffix = null;
        switch (this) {
            case MySql:
                suffix = "yearIsDateType=false&zeroDateTimeBehavior=convertToNull&rewriteBatchedStatements=true&tinyInt1isBit=false";
                if (jdbc.contains("?")) {
                    result = jdbc + "&" + suffix;
                } else {
                    result = jdbc + "?" + suffix;
                }
                break;
            case DRDS:
                suffix = "yearIsDateType=false&zeroDateTimeBehavior=convertToNull";
                if (jdbc.contains("?")) {
                    result = jdbc + "&" + suffix;
                } else {
                    result = jdbc + "?" + suffix;
                }
                break;
            case Oracle:
                break;
            case SQLServer:
                break;
            case DB2:
                break;
            case PostgreSQL:
                break;
            case LIBRA:
                break;
            case RDBMS:
                break;
            default:
                throw new DtCenterDefException("unsupported database type.", DBErrorCode.UNSUPPORTED_TYPE);
        }

        return result;
    }

    public String formatPk(String splitPk) {
        String result = splitPk;

        switch (this) {
            case MySql:
            case Oracle:
                if (splitPk.length() >= 2 && splitPk.startsWith("`") && splitPk.endsWith("`")) {
                    result = splitPk.substring(1, splitPk.length() - 1).toLowerCase();
                }
                break;
            case SQLServer:
                if (splitPk.length() >= 2 && splitPk.startsWith("[") && splitPk.endsWith("]")) {
                    result = splitPk.substring(1, splitPk.length() - 1).toLowerCase();
                }
                break;
            case DB2:
            case PostgreSQL:
            case LIBRA:
                break;
            default:
                throw new DtCenterDefException("unsupported database type.", DBErrorCode.UNSUPPORTED_TYPE);
        }

        return result;
    }


    public String quoteColumnName(String columnName) {
        String result = columnName;

        switch (this) {
            case MySql:
                result = "`" + columnName.replace("`", "``") + "`";
                break;
            case Oracle:
                break;
            case SQLServer:
                result = "[" + columnName + "]";
                break;
            case DB2:
            case PostgreSQL:
            case LIBRA:
                break;
            default:
                throw new DtCenterDefException("unsupported database type.", DBErrorCode.UNSUPPORTED_TYPE);
        }

        return result;
    }

    public String quoteTableName(String tableName) {
        String result = tableName;

        switch (this) {
            case MySql:
                result = "`" + tableName.replace("`", "``") + "`";
                break;
            case Oracle:
                break;
            case SQLServer:
                break;
            case DB2:
                break;
            case PostgreSQL:
                break;
            case LIBRA:
                break;
            default:
                throw new DtCenterDefException("unsupported database type.", DBErrorCode.UNSUPPORTED_TYPE);
        }

        return result;
    }

    private static Pattern mysqlPattern = Pattern.compile("jdbc:mysql://(.+):\\d+/.+");
    private static Pattern oraclePattern = Pattern.compile("jdbc:oracle:thin:@(.+):\\d+:.+");

    /**
     * 注意：目前只实现了从 mysql/oracle 中识别出ip 信息.未识别到则返回 null.
     */
    public static String parseIpFromJdbcUrl(String jdbcUrl) {
        Matcher mysql = mysqlPattern.matcher(jdbcUrl);
        if (mysql.matches()) {
            return mysql.group(1);
        }
        Matcher oracle = oraclePattern.matcher(jdbcUrl);
        if (oracle.matches()) {
            return oracle.group(1);
        }
        return null;
    }
    public String getTypeName() {
        return typeName;
    }

    public void setTypeName(String typeName) {
        this.typeName = typeName;
    }
}
