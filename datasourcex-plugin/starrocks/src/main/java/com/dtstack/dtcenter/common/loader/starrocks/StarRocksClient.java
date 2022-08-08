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

package com.dtstack.dtcenter.common.loader.starrocks;

import com.dtstack.dtcenter.common.loader.mysql5.MysqlClient;
import com.dtstack.dtcenter.common.loader.rdbms.ConnFactory;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * @author qiuyun
 * @version 1.0
 * @date 2022-07-21 16:12
 */
public class StarRocksClient extends MysqlClient {

    /**
     * 获取配置的大小写是否敏感
     * 备注：starRocks 表名大小写敏感
     */
    private static final String SHOW_CASE_VARIABLES = "show variables like '%lower_case_table_names%'";

    /**
     * 判断table是否在schema中，根据lower_case_table_names参数值不同匹配不同的sql
     */
    private static final String TABLE_IS_IN_SCHEMA_0 = "select table_name from information_schema.tables where table_schema='%s' and table_name = '%s'";
    private static final String TABLE_IS_IN_SCHEMA_1 = "select table_name from information_schema.tables where table_schema='%s' and table_name = LOWER('%s')";
    private static final String TABLE_IS_IN_SCHEMA_2 = "SELECT small_name FROM ( SELECT lower( table_name ) AS small_name FROM information_schema.TABLES WHERE table_schema = '%s' ) AS temp WHERE temp.small_name = lower('%s')";


    @Override
    protected ConnFactory getConnFactory() {
        return new StarRocksConnFactory();
    }

    @Override
    protected DataSourceType getSourceType() {
        return DataSourceType.STAR_ROCKS;
    }

    @Override
    public Boolean isTableExistsInDatabase(ISourceDTO source, String tableName, String dbName) {
        if (StringUtils.isBlank(dbName)) {
            throw new DtLoaderException("database name is not empty");
        }

        //先查询大小写是否敏感,mysql默认情况下为0
        List<Map<String, Object>> mapList = executeQuery(source, SqlQueryDTO.builder().sql(SHOW_CASE_VARIABLES).build());
        if (CollectionUtils.isEmpty(mapList)) {
            throw new DtLoaderException("mysql lower case setting is null");
        }
        Integer caseCode = MapUtils.getInteger(mapList.get(0), "Value", 0);

        String sql = "";
        // 0: sql, 1：tableName.toLowerCase() , 2:select lower(table_name) from
        if (Objects.equals(caseCode, 0)) {
            sql = String.format(TABLE_IS_IN_SCHEMA_0, dbName, tableName);
        } else if (Objects.equals(caseCode, 1)) {
            sql = String.format(TABLE_IS_IN_SCHEMA_1, dbName, tableName);
        } else if (Objects.equals(caseCode, 2)) {
            sql = String.format(TABLE_IS_IN_SCHEMA_2, dbName, tableName);
        }

        return CollectionUtils.isNotEmpty(executeQuery(source, SqlQueryDTO.builder().sql(sql).build()));
    }
}
