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

package com.dtstack.dtcenter.common.loader.inceptor;

import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.hive.jdbc.HiveDriver;

import java.sql.Connection;
import java.sql.DriverPropertyInfo;
import java.util.Properties;

import static com.dtstack.dtcenter.common.loader.inceptor.metadata.cons.InceptorCons.CSVE_SERDE_LIBRARY;
import static com.dtstack.dtcenter.common.loader.inceptor.metadata.cons.InceptorCons.ELASTICSEARCH_SERDE_LIBRARY;
import static com.dtstack.dtcenter.common.loader.inceptor.metadata.cons.InceptorCons.HBASE_SERDE_LIBRARY;
import static com.dtstack.dtcenter.common.loader.inceptor.metadata.cons.InceptorCons.HYPERBASE_SERDE_LIBRARY;
import static com.dtstack.dtcenter.common.loader.inceptor.metadata.cons.InceptorCons.ORC_SERDE_LIBRARY;
import static com.dtstack.dtcenter.common.loader.inceptor.metadata.cons.InceptorCons.PARQUET_SERDE_LIBRARY;
import static com.dtstack.dtcenter.common.loader.inceptor.metadata.cons.InceptorCons.TEXTFILE_SERDE_LIBRARY;
import static com.dtstack.dtcenter.common.loader.inceptor.metadata.cons.InceptorCons.TYPE_CSVFILE;
import static com.dtstack.dtcenter.common.loader.inceptor.metadata.cons.InceptorCons.TYPE_ELASTICSEARCH;
import static com.dtstack.dtcenter.common.loader.inceptor.metadata.cons.InceptorCons.TYPE_HBASE;
import static com.dtstack.dtcenter.common.loader.inceptor.metadata.cons.InceptorCons.TYPE_HYPERBASE;
import static com.dtstack.dtcenter.common.loader.inceptor.metadata.cons.InceptorCons.TYPE_ORC;
import static com.dtstack.dtcenter.common.loader.inceptor.metadata.cons.InceptorCons.TYPE_PARQUET;
import static com.dtstack.dtcenter.common.loader.inceptor.metadata.cons.InceptorCons.TYPE_TEXT;

/**
 * inceptor驱动 工具类
 *
 * @author ：wangchuan
 * date：Created in 下午1:46 2020/11/6
 * company: www.dtstack.com
 */
@Slf4j
public class InceptorDriverUtil {

    private static final HiveDriver HIVE_DRIVER = new HiveDriver();

    /**
     * INCEPTOR_JDBC 前缀
     */
    private static final String JDBC_PREFIX = "jdbc:hive2://";

    /**
     * INCEPTOR_JDBC 前缀长度
     */
    private static final Integer JDBC_PREFIX_LENGTH = JDBC_PREFIX.length();

    /**
     * 解析 URL 配置信息
     *
     * @param url        数据源连接url
     * @param properties 数据源配置
     * @return 配置详细信息
     */
    private static DriverPropertyInfo[] parseProperty(String url, Properties properties) {
        try {
            return HIVE_DRIVER.getPropertyInfo(url, properties);
        } catch (Exception e) {
            throw new DtLoaderException(String.format("Spark parse URL exception : %s", e.getMessage()), e);
        }
    }

    /**
     * 获取 Schema 信息
     *
     * @param url 数据源连接url
     * @return 从url中解析出 schema 信息
     */
    private static String getSchema(String url) {
        return parseProperty(url, null)[2].value;
    }

    /**
     * 设置 Schema 信息
     *
     * @param conn 数据源连接
     * @param url  数据源连接url
     * @return 设置schema后的数据源连接
     */
    public static Connection setSchema(Connection conn, String url, String schema) {
        String schemaSet = StringUtils.isBlank(schema) ? getSchema(url) : schema;
        if (StringUtils.isBlank(schemaSet)) {
            return conn;
        }
        try {
            conn.setSchema(schemaSet);
        } catch (Exception e) {
            throw new DtLoaderException(String.format("Setting schema exception : %s", e.getMessage()), e);
        }
        return conn;
    }

    /**
     * 去除 Schema 信息
     *
     * @param url 数据源连接url
     * @return 去除 Schema 信息的数据源连接url
     */
    public static String removeSchema(String url) {
        String schema = getSchema(url);
        return removeSchema(url, schema);
    }

    /**
     * 去除 Schema 信息
     *
     * @param url    数据源连接url
     * @param schema schema信息
     * @return 去除 Schema 信息的数据源连接url
     */
    private static String removeSchema(String url, String schema) {
        if (StringUtils.isBlank(schema) || !url.toLowerCase().contains(JDBC_PREFIX)) {
            return url;
        }
        String urlWithoutPrefix = url.substring(JDBC_PREFIX_LENGTH);
        return JDBC_PREFIX + urlWithoutPrefix.replaceFirst("/" + schema, "/");
    }

    /**
     * 获取hive store的方式
     *
     * @param storedClass str
     * @return stored type
     */
    public static String getStoredType(String storedClass) {
        if (storedClass.endsWith(TEXTFILE_SERDE_LIBRARY)) {
            return TYPE_TEXT;
        } else if (storedClass.endsWith(ORC_SERDE_LIBRARY)) {
            return TYPE_ORC;
        } else if (storedClass.endsWith(PARQUET_SERDE_LIBRARY)) {
            return TYPE_PARQUET;
        } else if (storedClass.endsWith(CSVE_SERDE_LIBRARY)) {
            return TYPE_CSVFILE;
        } else if (storedClass.endsWith(ELASTICSEARCH_SERDE_LIBRARY)) {
            return TYPE_ELASTICSEARCH;
        } else if (storedClass.endsWith(HBASE_SERDE_LIBRARY)) {
            return TYPE_HBASE;
        } else if (storedClass.endsWith(HYPERBASE_SERDE_LIBRARY)) {
            return TYPE_HYPERBASE;
        } else {
            return storedClass;
        }
    }
}
