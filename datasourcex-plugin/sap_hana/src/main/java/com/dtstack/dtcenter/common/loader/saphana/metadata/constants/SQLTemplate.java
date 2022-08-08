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

package com.dtstack.dtcenter.common.loader.saphana.metadata.constants;


/**
 * @author zhiyi
 */
public class SQLTemplate {
    public static final String QUERY_DATABASE = "SELECT DATABASE_NAME FROM \"SYS\".\"M_DATABASE\"";
    public static final String QUERY_SCHEMAS = "SELECT SCHEMA_NAME FROM SCHEMAS";
    public static final String QUERY_TABLES = "SELECT TABLE_NAME FROM TABLES WHERE SCHEMA_NAME = '%s'";
    public static final String QUERY_TABLE = "SELECT t1.TABLE_NAME, t1.RECORD_COUNT, t1.TABLE_SIZE, t1.TABLE_TYPE, t2.COMMENTS\n" +
            "FROM \"SYS\".\"M_TABLES\" t1, \"TABLES\" t2 WHERE t1.SCHEMA_NAME = t2.SCHEMA_NAME \n" +
            "AND t1.TABLE_NAME = t2.TABLE_NAME AND t1.SCHEMA_NAME = '%s' AND t1.TABLE_NAME = '%s'";
    public static final String QUERY_TABLE_CREATE_TIME = "SELECT CREATE_TIME FROM \"SYS\".\"M_CS_TABLES\" WHERE SCHEMA_NAME = '%s' AND TABLE_NAME = '%s' LIMIT 1";
    public static final String QUERY_COLUMNS = "SELECT COLUMN_NAME, POSITION, DATA_TYPE_NAME, LENGTH, DEFAULT_VALUE, COMMENTS FROM TABLE_COLUMNS WHERE SCHEMA_NAME = '%s' AND TABLE_NAME = '%s'";
    public static final String QUERY_INDEXES = "SELECT INDEX_NAME, COLUMN_NAME, CONSTRAINT, POSITION FROM INDEX_COLUMNS WHERE SCHEMA_NAME = '%s' AND TABLE_NAME = '%s'";
    public static final String QUERY_VIEWS = "SELECT VIEW_NAME, VIEW_TYPE, DEFINITION, IS_VALID FROM VIEWS WHERE SCHEMA_NAME = '%s'";
    public static final String QUERY_VIEW_COLUMNS = "";
}
