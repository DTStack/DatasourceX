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

package com.dtstack.dtcenter.common.loader.postgresql.metadata.utils;

import com.dtstack.dtcenter.common.loader.common.utils.JdbcUrlUtil;

import java.util.regex.Matcher;

/**
 * @author zhiyi
 */
public class CommonUtils {

    public static final String URL_NO_SCHEMA = "jdbc:postgresql://{host}[:{port}]";

    public static final String URL_WITH_SCHEMA = "jdbc:postgresql://{host}[:{port}]/[{database}]";

    public static final String URL_WITH_PARAMS = "jdbc:postgresql://{host}[:{port}]/[{database}][\\?{params}]";

    public static final String URL_WITH_SCHEMA_TEMPLATE = "jdbc:postgresql://%s:%s/%s";

    public static final String URL_WITH_PARAMS_TEMPLATE = "jdbc:postgresql://%s:%s/%s?%s";


    /**
     * 用dbName替换url中的dbName
     *
     * @param url:    jdbc地址
     * @param dbName: 数据库名
     * @return 新的url
     **/
    public static String dbUrlTransform(String url, String dbName) {
        Matcher matcher;
        if ((matcher = JdbcUrlUtil.getPattern(URL_NO_SCHEMA).matcher(url)).matches()) {
            String host = matcher.group(JdbcUrlUtil.PROP_HOST);
            String port = matcher.group(JdbcUrlUtil.PROP_PORT);
            return String.format(URL_WITH_SCHEMA_TEMPLATE, host, port, dbName);
        } else if ((matcher = JdbcUrlUtil.getPattern(URL_WITH_SCHEMA).matcher(url)).matches()) {
            String host = matcher.group(JdbcUrlUtil.PROP_HOST);
            String port = matcher.group(JdbcUrlUtil.PROP_PORT);
            return String.format(URL_WITH_SCHEMA_TEMPLATE, host, port, dbName);
        } else if ((matcher = JdbcUrlUtil.getPattern(URL_WITH_PARAMS).matcher(url)).matches()) {
            String host = matcher.group(JdbcUrlUtil.PROP_HOST);
            String port = matcher.group(JdbcUrlUtil.PROP_PORT);
            String params = matcher.group(JdbcUrlUtil.PROP_PARAMS);
            return String.format(URL_WITH_PARAMS_TEMPLATE, host, port, dbName, params);
        }
        return url;
    }

    /**
     * 根据创建索引的SQL语句得到索引类型
     *
     * @param sql: SQL语句
     * @return 索引类型
     **/
    public static String indexType(String sql) {
        //sql:CREATE UNIQUE INDEX xxxx ON xxxx USING btree (id)
        String result = sql.toLowerCase();
        String[] s = result.split(" ");
        for (int i = 0; i < s.length; i++) {
            if ("using".equals(s[i])) {
                result = s[i + 1];
            }
        }
        return result;
    }

}
