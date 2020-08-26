package com.dtstack.dtcenter.common.loader.common;

import lombok.NoArgsConstructor;

@NoArgsConstructor
public class SqlFormatUtil {
    private String sql;

    /**
     * 初始化
     *
     * @param sql
     * @return
     */
    public static SqlFormatUtil init(String sql) {
        SqlFormatUtil util = new SqlFormatUtil();
        util.sql = sql;
        return util;
    }

    /**
     * 格式化 SQL
     *
     * @param sql
     * @return
     */
    public static String formatSql(String sql) {
        return init(sql).removeComment().toOneLine().removeBlank().removeEndChar().getSql();
    }

    /**
     * 去除换行符，变为一行
     *
     * @return
     */
    public SqlFormatUtil toOneLine() {
        this.sql = this.sql.replaceAll("\r", " ").replaceAll("\n", " ");
        return this;
    }

    /**
     * 去除末尾分号
     *
     * @return
     */
    public SqlFormatUtil removeEndChar() {
        this.sql = this.sql.trim();
        if (this.sql.endsWith(";")) {
            this.sql = this.sql.substring(0, this.sql.length() - 1);
        }

        return this;
    }

    /**
     * 去除注释
     *
     * @return
     */
    public SqlFormatUtil removeComment() {
        this.sql = this.sql.replaceAll("--.*", "");
        this.sql = this.sql.replaceAll("\\/\\*\\*+.*\\*\\*+\\/", "");
        this.sql = this.sql.replaceAll("/\\*{1,2}\\*/", "");
        this.sql = this.sql.replaceAll("/\\*{1,2}[\\s\\S]*?\\*/", "");
        return this;
    }

    /**
     * 去除空格
     *
     * @return
     */
    public SqlFormatUtil removeBlank() {
        this.sql = this.sql.trim();
        return this;
    }

    /**
     * 获取 SQL
     *
     * @return
     */
    public String getSql() {
        return this.sql;
    }
}
