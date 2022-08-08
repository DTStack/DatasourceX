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

package com.dtstack.dtcenter.common.loader.phoenix5.metadata.constants;

import com.dtstack.dtcenter.common.loader.rdbms.metadata.constants.RdbCons;

/**
 * @author zhiyi
 */
public class PhoenixMetadataCons extends RdbCons {

    public static final String KEY_PATH = "path";

    public static final String KEY_DEFAULT = "default";

    public static final String DRIVER_NAME = "org.apache.phoenix.jdbc.PhoenixDriver";

    public static final String SQL_DEFAULT_TABLE_NAME = " SELECT DISTINCT TABLE_NAME FROM SYSTEM.CATALOG WHERE TABLE_SCHEM is null ";

    public static final String SQL_TABLE_NAME = " SELECT DISTINCT TABLE_NAME FROM SYSTEM.CATALOG WHERE TABLE_SCHEM = '%s' ";

    public static final String SQL_COLUMN = "SELECT ORDINAL_POSITION, COLUMN_FAMILY FROM SYSTEM.CATALOG WHERE TABLE_SCHEM = '%s' AND TABLE_NAME = '%s' ";

    public static final String SQL_DEFAULT_COLUMN = "SELECT ORDINAL_POSITION, COLUMN_FAMILY FROM SYSTEM.CATALOG WHERE TABLE_SCHEM is null AND TABLE_NAME = '%s' ";

    public static final String HBASE_MASTER_KERBEROS_PRINCIPAL = "hbase.master.kerberos.principal";

    public static final String HBASE_REGIONSERVER_KERBEROS_PRINCIPAL = "hbase.regionserver.kerberos.principal";

    public static final String PHOENIX_QUERYSERVER_KERBEROS_PRINCIPAL = "phoenix.queryserver.kerberos.principal";

    public final static String HBASE_SECURITY_AUTHENTICATION = "hbase.security.authentication";

    public final static String HBASE_SECURITY_AUTHORIZATION = "hbase.security.authorization";


    public final static String HADOOP_SECURITY_AUTHENTICATION = "hadoop.security.authentication";

    public final static String KEY_PRINCIPAL = "principal";

    public static final String KEY_HADOOP_CONFIG = "hadoopConfig";

    public static final String AUTHENTICATION_TYPE = "kerberos";

    public static final String KEYTAB_FILE = "keytabFileName";

    public static final String RESULT_SET_TABLE_NAME = "TABLE_NAME";

    public static final String SINGLE_SLASH_SYMBOL = "/";

    public static final String POINT_SYMBOL = ".";

    public static final String COLON_SYMBOL = ":";


}
