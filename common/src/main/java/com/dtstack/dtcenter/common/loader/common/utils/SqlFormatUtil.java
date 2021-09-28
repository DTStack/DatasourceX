/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dtstack.dtcenter.common.loader.common.utils;

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
