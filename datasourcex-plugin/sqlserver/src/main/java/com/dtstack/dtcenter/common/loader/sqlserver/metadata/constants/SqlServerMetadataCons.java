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

package com.dtstack.dtcenter.common.loader.sqlserver.metadata.constants;

import com.dtstack.dtcenter.common.loader.rdbms.metadata.constants.RdbCons;

public class SqlServerMetadataCons extends RdbCons {

    public static final String DRIVER_NAME = "net.sourceforge.jtds.jdbc.Driver";

    public static final String KEY_SCHEMA_NAME = "schemaName";

    public static final String KEY_TABLE_NAME = "tableName";

    public static final String SQL_SWITCH_schema = "USE \"%s\"";

    //public static final String SQL_SWITCH_schema = "EXECUTE as USER='%s'";

//    public static final String KEY_ZERO = "0";

    /**拼接成schema.table*/
    public static final String SQL_SHOW_TABLES = "SELECT OBJECT_SCHEMA_NAME(object_id, DB_ID()) as SCHEMA_NAME, name FROM sys.tables";

    public static final String SQL_SHOW_TABLE_PROPERTIES = "SELECT a.crdate, b.rows, rtrim(8*dpages) used, ep.value \n" +
            "FROM sysobjects AS a INNER JOIN sysindexes AS b ON a.id = b.id \n" +
            "LEFT JOIN sys.extended_properties AS ep ON a.id = ep.major_id AND ep.minor_id = 0 \n" +
            "WHERE (a.type = 'u') AND (b.indid IN (0, 1)) and a.name = %s AND OBJECT_SCHEMA_NAME(a.id, DB_ID()) = %s ";

    public static final String SQL_SHOW_TABLE_INDEX = "SELECT a.name, d.name as columnName, type_desc as type \n" +
            "FROM sys.indexes a JOIN sysindexkeys b ON a.object_id=b.id AND a.index_id=b.indid \n" +
            "JOIN sysobjects c ON b.id=c.id JOIN syscolumns d ON b.id=d.id AND b.colid=d.colid \n" +
            "WHERE c.name=%s and OBJECT_SCHEMA_NAME(A.object_id, DB_ID())=%s \n" +
            "AND  a.index_id  NOT IN(0,255)";

    public static final String SQL_SHOW_PARTITION_COLUMN = "SELECT b.name\n" +
            "FROM sys.index_columns a JOIN sys.columns b ON a.object_id = b.object_id and a.column_id = b.column_id \n" +
            "JOIN sys.objects c ON c.object_id = a.object_id \n" +
            "WHERE a.partition_ordinal <> 0 AND c.name =%s \n" +
            "AND OBJECT_SCHEMA_NAME(a.object_id , DB_ID())=%s ";

    public static final String SQL_SHOW_PARTITION = "select ps.name, p.rows, pf.create_date, ds2.name as filegroup \n" +
            "from sys.indexes i join sys.partition_schemes ps on i.data_space_id = ps.data_space_id \n" +
            "join sys.destination_data_spaces dds on ps.data_space_id = dds.partition_scheme_id \n" +
            "join sys.data_spaces ds2 on dds.data_space_id = ds2.data_space_id \n" +
            "join sys.partitions p on dds.destination_id = p.partition_number \n" +
            "and p.object_id = i.object_id and p.index_id = i.index_id \n" +
            "join sys.partition_functions pf on ps.function_id = pf.function_id \n" +
            "WHERE i.object_id = object_id(%s) and OBJECT_SCHEMA_NAME(i.object_id, DB_ID())=%s \n" +
            "and i.index_id in (0, 1)";


//    public static final String SQL_SHOW_TABLE_COLUMN = "SELECT B.name AS name, TY.name as type, C.value AS comment, B.is_nullable as nullable, COLUMNPROPERTY(B.object_id ,B.name,'PRECISION') as presice, D.text, B.column_id \n" +
//            "FROM sys.tables A INNER JOIN sys.columns B ON B.object_id = A.object_id \n" +
//            "INNER JOIN sys.types TY ON B.system_type_id = TY.system_type_id \n" +
//            "LEFT JOIN sys.extended_properties C ON C.major_id = B.object_id AND C.minor_id = B.column_id \n" +
//            "left join syscomments D on A.object_id=D.id " +
//            "WHERE A.name = %s and OBJECT_SCHEMA_NAME(A.object_id, DB_ID())=%s";

    public static final String SQL_QUERY_COLUMN_LENGTH = "SELECT name,COL_LENGTH(%s,name) AS length FROM syscolumns WHERE ID = OBJECT_ID(%s)";

    public static final String SQL_QUERY_PRIMARY_KEY = "SELECT ku.COLUMN_NAME\n" +
            "FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS tc\n" +
            "INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS ku\n" +
            "ON tc.CONSTRAINT_TYPE = 'PRIMARY KEY' \n" +
            "AND tc.CONSTRAINT_NAME = ku.CONSTRAINT_NAME\n" +
            "WHERE ku.TABLE_NAME = %s AND ku.TABLE_SCHEMA = %s";


}
