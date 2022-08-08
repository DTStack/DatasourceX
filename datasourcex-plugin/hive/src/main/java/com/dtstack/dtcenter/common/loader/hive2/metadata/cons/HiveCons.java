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

package com.dtstack.dtcenter.common.loader.hive2.metadata.cons;

/**
 * hive2.x使用常量类
 *
 * @author luming
 * @date 2022/4/14
 */
public class HiveCons {
    public static final String SQL_SWITCH_DATABASE = "USE %s";

    public static final String SQL_SHOW_TABLES = "SHOW TABLES";

    public static final String DRIVER_NAME = "org.apache.hive.jdbc.HiveDriver";
    public static final String KEY_HADOOP_CONFIG = "hadoopConfig";

    public static final String KEY_SOURCE = "source";
    public static final String KEY_VERSION = "version";

    public static final String TEXT_FORMAT = "TextOutputFormat";
    public static final String ORC_FORMAT = "OrcOutputFormat";
    public static final String PARQUET_FORMAT = "MapredParquetOutputFormat";

    public static final String TYPE_TEXT = "TEXTFILE";
    public static final String TYPE_ORC = "orc";
    public static final String TYPE_PARQUET = "parquet";

    public static final String PARTITION_INFORMATION = "# Partition Information";
    public static final String TABLE_INFORMATION = "# Detailed Table Information";

    // desc formatted后的列名
    public static final String KEY_RESULTSET_COL_NAME = "# col_name";
    public static final String KEY_RESULTSET_DATA_TYPE = "data_type";
    public static final String KEY_COMMENT = "comment";

    public static final String KEY_COL_LOCATION = "Location:";
    public static final String KEY_COL_CREATETIME = "CreateTime:";
    public static final String KEY_COL_CREATE_TIME = "Create Time:";
    public static final String KEY_COL_LASTACCESSTIME = "LastAccessTime:";
    public static final String KEY_COL_LAST_ACCESS_TIME = "Last Access Time:";
    public static final String KEY_COL_OUTPUTFORMAT = "OutputFormat:";
    public static final String KEY_COL_TABLE_PARAMETERS = "Table Parameters:";

    public static final String KEY_LOCATION = "location";
    public static final String KEY_CREATETIME = "createTime";
    public static final String KEY_LASTACCESSTIME = "lastAccessTime";
    public static final String KEY_TOTALSIZE = "totalSize";
    public static final String KEY_NUMROWS = "numRows";
    public static final String KEY_SPARK_TOTALSIZE = "spark.sql.statistics.totalSize";
    public static final String KEY_SPARK_NUMROWS = "spark.sql.statistics.numRows";
    public static final String KEY_TRANSIENT_LASTDDLTIME = "transient_lastDdlTime";

    public static final String KEY_NAME = "name";
    public static final String KEY_VALUE = "value";


    public static final String SQL_QUERY_DATA = "desc formatted %s";
    public static final String SQL_SHOW_PARTITIONS = "show partitions %s";
    public static final String KEY_COLUMN_DATA_TYPE = "data_type";
    public static final String KEY_COL_NAME = "col_name";

    public static final String KEY_PRINCIPAL = "principal";

    public static final String ANALYZE_TABLE = "analyze table %s %s compute statistics";
}
