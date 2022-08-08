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

package com.dtstack.dtcenter.common.loader.doris.metadata.cons;

/**
 * doris使用常量类
 *
 * @author luming
 * @date 2022/4/11
 */
public class DorisConstants {
    /**
     * http请求成功标识
     */
    public static final String SUCCESS_FLAG = "success";

    /**
     * dorisrestful 默认群集名称
     */
    public static final String DEFAULT_CLUSTER = "default_cluster";

    /**
     * Get. 查询指定数据库下表的存储格式是否为行存储格式（行存储格式已经废弃）
     */
    public static final String QUERY_DB_STORE_TYPE = "/api/_check_storagetype?db=%s";

    /**
     * 获取指定表的建表语句、建分区语句和建rollup语句
     */
    public static final String QUERY_CREATE_TABLE = "/api/_get_ddl?db=%s&table=%s";

    /**
     * 获取当前namespace下的库名，按字母排序
     */
    public static final String QUERY_DATABASE_LIST = "/api/meta/namespaces/%s/databases";

    /**
     * 获取当前数据库下的表名，按字母序排列
     */
    public static final String QUERY_TABLE_LIST = "/api/meta/namespaces/%s/databases/%s/tables";

    /**
     * 更新指定表的行数统计信息，在更新行数统计信息的同时，也会以JSON格式返回表以及对应rollup的行数
     */
    public static final String QUERY_ROW_COUNT = "/api/rowcount?db=%s&table=%s";

    /**
     * 获取指定namespace下指定库指定表的表结构信息
     */
    public static final String QUERY_TABLE_INFO =
            "/api/meta/namespaces/%s/databases/%s/tables/%s/schema";

    /**
     * JDBC. 切换数据库
     */
    public static final String SQL_SWITCH_DATABASE = "USE `%s`;";

    /**
     * JDBC. 查询当前库下的所有表
     */
    public static final String SQL_SHOW_TABLE = "SHOW TABLES;";

    /**
     * JDBC. 查询表索引
     */
    public static final String SQL_SHOW_INDEX = "SHOW INDEX FROM `%s`.`%s`;";

    /**
     * JDBC. 查看指定表的列信息
     */
    public static final String SQL_SHOW_FULL_COLUMN = "SHOW FULL COLUMNS FROM `%s`.`%s`;";

    /**
     * JDBC.查看数据库下所有的自定义(系统提供)的函数。如果用户指定了数据库，那么查看对应数据库的，否则直接查询当前会话所在数据库
     */
    public static final String SQL_SHOW_FULL_FUNC = "SHOW FULL FUNCTIONS IN %s";

    /**
     * JDBC. 用于展示数据量、副本数量以及统计行数
     */
    public static final String SQL_SHOW_DATA = "SHOW DATA FROM `%s`.`%s`;";

    public static final String SQL_SHOW_INFORMATION =
            "SELECT * FROM `INFORMATION_SCHEMA`.`TABLES` WHERE TABLE_SCHEMA='%s' AND TABLE_NAME='%s'";
}
