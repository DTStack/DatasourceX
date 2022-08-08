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

package com.dtstack.dtcenter.loader.dto.canal;

/**
 * @author by zhiyi
 * @date 2022/3/21 3:58 下午
 */
public class BinlogConstant {

    public static final String JDBC_URL_TEMPLATE = "jdbc:mysql://%s";

    /**
     * 默认的destination
     */
    public static final String DEFAULT_DESTINATION = "example";

    public static final String SCHEMA_TABLE_FORMAT = "%s.%s";

    public static final String SCHEMA_FORMAT = "%s\\\\..*";

    public static final String NO_HOST_TIP = "No host supplied;\n";

    public static final String NO_USERNAME_TIP = "No username supplied;\n";

    public static final String NO_URL_TIP = "No url supplied;\n";

    public static final String NO_CAT_TIP = "No cat supplied;\n";

    public static final String DEFAULT_CAT = "INSERT,UPDATE,DELETE";

    public static final String BIN_LOG_TIP_TEMPLATE = "binlog cat not support-> %s,just support->%s;\n";

    public static final String TIMESTAMP = "timestamp";

    public static final String BIN_LOG_START_TIMESTAMP_TIP_TEMPLATE = "binlog start parameter of timestamp  must be long type, but your value is -> %s;\n";

    public static final String POSITION = "position";

    public static final String BIN_LOG_START_POSITION_TIP_TEMPLATE = "binlog start parameter of position  must be long type, but your value is -> %s;\n";

    public static final String USER_PRIVILEGE_TIP_TEMPLATE = "\nyou need (at least one of) the SUPER,REPLICATION CLIENT privilege(s) for this operation; you can execute sql ->GRANT SELECT, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO %s IDENTIFIED BY password;\n\n";

    public static final String BIN_LOG_NOT_ENABLED_TIP = "binlog has not enabled, please click my.cnf Add the following to the file: \n server_id=109\n log_bin = /var/lib/mysql/mysql-bin\n binlog_format = ROW\n expire_logs_days = 30\n\n";

    public static final String BIN_LOG_FORMAT_TIP = " binlog_format must be set ROW ;\n";

    public static final String USER_SELECT_PRIVILEGE_TEMPLATE = "user has not select privilege on %s";

    public static final String BIN_LOG_CONFIG_NOT_RIGHT_TIP_TEMPLATE = " binlog config not right，details is  %s";

    public static final String ERROR_CHECK_BIN_LOG_TEMPLATE = "\n error to check binlog config, e = %s";

    public static final String JOURNAL_NAME = "journalName";
}
