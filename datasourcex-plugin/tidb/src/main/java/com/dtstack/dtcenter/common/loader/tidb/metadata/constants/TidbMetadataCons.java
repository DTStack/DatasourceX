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

package com.dtstack.dtcenter.common.loader.tidb.metadata.constants;

import com.dtstack.dtcenter.common.loader.rdbms.metadata.constants.RdbCons;

public class TidbMetadataCons extends RdbCons {

    public static final String DRIVER_NAME = "com.mysql.jdbc.Driver";
    public static final String KEY_UPDATE_TIME = "updateTime";


    public static final String RESULT_ROWS = "Rows";
    public static final String RESULT_DATA_LENGTH = "Data_length";
    public static final String RESULT_FIELD = "Field";
    public static final String RESULT_TYPE = "Type";
    public static final String RESULT_COLUMN_NULL = "COLUMN_NULL";
    public static final String RESULT_KEY = "Key";
    public static final String RESULT_COLUMN_DEFAULT = "COLUMN_DEFAULT";
    public static final String RESULT_COLUMN_PRIMARY_KEY = "COLUMN_PRIMARY_KEY";
    public static final String RESULT_COLUMN_NAME = "COLUMN_NAME";
    public static final String RESULT_COLUMN_TYPE = "COLUMN_TYPE";
    public static final String RESULT_COLUMN_COMMENT = "COLUMN_COMMENT";
    public static final String RESULT_COLUMN_POSITION = "COLUMN_POSITION";
    public static final String RESULT_COLUMN_PRECISION = "COLUMN_PRECISION";
    public static final String RESULT_COLUMN_SCALE = "COLUMN_SCALE";
    public static final String RESULT_PARTITION_NAME = "PARTITION_NAME";
    public static final String RESULT_PARTITION_CREATE_TIME = "CREATE_TIME";
    public static final String RESULT_PARTITION_TABLE_ROWS = "TABLE_ROWS";
    public static final String RESULT_PARTITION_DATA_LENGTH = "DATA_LENGTH";
    public static final String RESULT_PARTITIONNAME = "Partition_name";
    public static final String RESULT_PARTITION_EXPRESSION = "PARTITION_EXPRESSION";
    public static final String RESULT_CREATE_TIME = "Create_time";
    public static final String RESULT_UPDATE_TIME = "Update_time";
    public static final String RESULT_COMMENT = "Comment";

    /** sql语句 */
    public static final String SQL_SHOW_TABLES = "SHOW FULL TABLES WHERE Table_type = 'BASE TABLE'";
    public static final String SQL_QUERY_TABLE_INFO = "SHOW TABLE STATUS LIKE '%s'";
    public static final String SQL_QUERY_COLUMN = "SELECT\n" +
            "\tCOLUMN_NAME,\n" +
            "\tORDINAL_POSITION as COLUMN_POSITION,\n" +
            "\tNUMERIC_PRECISION as COLUMN_PRECISION,\n" +
            "\tNUMERIC_SCALE as COLUMN_SCALE,\n" +
            "\tCOLUMN_TYPE,\n" +
            "\tIS_NULLABLE as COLUMN_NULL,\n" +
            "\tCOLUMN_KEY as COLUMN_PRIMARY_KEY,\n" +
            "\tCOLUMN_DEFAULT,\n" +
            "\tCOLUMN_COMMENT\n" +
            "FROM\n" +
            "\tINFORMATION_SCHEMA.COLUMNS \n" +
            "WHERE\n" +
            "\tTABLE_NAME = \"%s\"\n" +
            "\tAND TABLE_SCHEMA = \"%s\"\n";
    public static final String SQL_QUERY_UPDATE_TIME = "SHOW STATS_META WHERE Table_name = '%s'";
    public static final String SQL_QUERY_PARTITION = "SELECT * FROM information_schema.partitions WHERE table_schema = schema() AND table_name='%s'";
    public static final String SQL_QUERY_PARTITION_COLUMN = "SELECT DISTINCT PARTITION_EXPRESSION FROM information_schema.partitions WHERE table_schema = schema() AND table_name='%s'";
}
