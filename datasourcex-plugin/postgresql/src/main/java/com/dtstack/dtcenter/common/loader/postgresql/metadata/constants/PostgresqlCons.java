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

package com.dtstack.dtcenter.common.loader.postgresql.metadata.constants;

import com.dtstack.dtcenter.common.loader.rdbms.metadata.constants.RdbCons;


/**
 * @author zhiyi
 */
public class PostgresqlCons extends RdbCons {

    /**
     * 驱动
     */
    public static final String DRIVER_NAME = "org.postgresql.Driver";

    /**
     * 表名
     */
    public static final String KEY_TABLE_NAME = "tableName";

    /**
     * schemaName
     */
    public static final String KEY_SCHEMA_NAME = "schemaName";

    /**
     * 设置搜索路径
     */
    public static final String SQL_SET_SEARCHPATH = "set search_path = %s";

    /**
     * sql语句：查询数据库中所有的表名
     */
    public static final String SQL_SHOW_TABLES = "select table_schema,table_name from information_schema.tables where table_schema <> 'information_schema' and  table_schema <> 'pg_catalog'";

    /**
     * sql语句：查询表中共有多少条数据（包含null值）
     */
    public static final String SQL_SHOW_COUNT = "select count(1) as count from %s";

    /**
     * 查询索引名及创建索引的SQL语句
     */
    public static final String SQL_SHOW_INDEX = "select schemaname,tablename,indexname,indexdef from pg_indexes where schemaname = '%s' and tablename = '%s'";

    /**
     * sql语句：查询表所占磁盘空间大小
     */
    public static final String SQL_SHOW_TABLE_SIZE = "select pg_relation_size('%s.%s') as size";

    /**
     * table_schema
     */
    public static final String TABLE_SCHEMA = "table_schema";

    /**
     * size
     */
    public static final String SIZE = "size";

    public static final String COLUMN_NAME = "COLUMN_NAME";

    public static final String COUNT = "count";

    public static final String INDEX_NAME = "INDEX_NAME";

    public static final String INDEXNAME = "indexname";

    public static final String INDEXDEF = "indexdef";

    public static final String TABLE_NAME = "table_name";

}
