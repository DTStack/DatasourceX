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

package com.dtstack.dtcenter.common.loader.vertica;

import com.dtstack.dtcenter.common.loader.common.utils.DBUtil;
import com.dtstack.dtcenter.common.loader.rdbms.AbsRdbmsClient;
import com.dtstack.dtcenter.common.loader.rdbms.ConnFactory;
import com.dtstack.dtcenter.loader.dto.ColumnMetaDTO;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;
import com.dtstack.dtcenter.loader.dto.source.VerticaSourceDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 12:02 2020/12/8
 * @Description：Vertica 实现类
 */
public class VerticaClient extends AbsRdbmsClient {
    /**
     * 展示所有的表
     */
    private static final String SHOW_TABLES = "select table_name AS table_name, table_schema AS schema_name from tables";

    /**
     * 展示特定 SCHEMA 的表
     */
    private static final String SHOW_TABLES_SCHEMA = "select table_name from tables where table_schema = '%s'";

    /**
     * 查找 schema
     */
    private static final String SHOW_SCHEMA = "select schema_name from schemata where is_system_schema = false";

    /**
     * 查找表注释
     */
    private static final String SHOW_TABLE_COMMENT = "select comment from comments where object_name= '%s'";

    /**
     * 查找表注释
     */
    private static final String SHOW_TABLE_COMMENT_WITH_SCHEMA = "select comment from comments where object_name= '%s' and object_schema = '%s'";

    /**
     * 数据预览
     */
    private static final String TABLE_QUERY = "SELECT * FROM %s";

    // 获取当前版本号
    private static final String SHOW_VERSION = "select version()";

    @Override
    public List<String> getTableList(ISourceDTO iSource, SqlQueryDTO queryDTO) {
        Integer clearStatus = beforeQuery(iSource, queryDTO, false);
        VerticaSourceDTO verticaSourceDTO = (VerticaSourceDTO) iSource;
        List<String> tableList = new ArrayList<>();
        try {
            List<Map<String, Object>> mapList = DBUtil.executeQuery(verticaSourceDTO.getConnection(), SHOW_TABLES);
            if (CollectionUtils.isEmpty(mapList)) {
                return Collections.emptyList();
            }

            for (Map<String, Object> map : mapList) {
                tableList.add(transferSchemaAndTable(MapUtils.getString(map, "schema_name"), MapUtils.getString(map, "table_name")));
            }
        } catch (Exception e) {
            throw new DtLoaderException(String.format("get table exception,%s", e.getMessage()), e);
        } finally {
            DBUtil.closeDBResources(null, null, DBUtil.clearAfterGetConnection(verticaSourceDTO, clearStatus));
        }
        return tableList;
    }

    @Override
    public List<String> getTableListBySchema(ISourceDTO iSource, SqlQueryDTO queryDTO) {
        if (StringUtils.isBlank(queryDTO.getSchema())) {
            return getTableList(iSource, queryDTO);
        }
        return super.getTableListBySchema(iSource, queryDTO);
    }

    @Override
    public String getTableMetaComment(ISourceDTO iSource, SqlQueryDTO queryDTO) {
        Integer clearStatus = beforeColumnQuery(iSource, queryDTO);
        VerticaSourceDTO verticaSourceDTO = (VerticaSourceDTO) iSource;

        try {
            String querySql;
            // 如果表传进来存在 schema 信息，需要过滤条件增加 schema 信息
            if (StringUtils.isNotBlank(queryDTO.getSchema())) {
                querySql = String.format(SHOW_TABLE_COMMENT_WITH_SCHEMA,
                        getTableFromTable(queryDTO.getTableName()), queryDTO.getSchema());
            } else if (StringUtils.isNotBlank(getSchemaFromTable(queryDTO.getTableName()))) {
                querySql = String.format(SHOW_TABLE_COMMENT_WITH_SCHEMA,
                        getTableFromTable(queryDTO.getTableName()), getSchemaFromTable(queryDTO.getTableName()));
            } else {
                querySql = String.format(SHOW_TABLE_COMMENT, getTableFromTable(queryDTO.getTableName()));
            }
            Map<String, Object> commentMap = DBUtil.executeQuery(verticaSourceDTO.getConnection(), querySql).get(0);
            return MapUtils.getString(commentMap, "comment");
        } catch (Exception e) {
            throw new DtLoaderException(String.format("get table: %s's information error. Please contact the DBA to check the database、table information.",
                    queryDTO.getTableName()), e);
        } finally {
            DBUtil.closeDBResources(null, null, DBUtil.clearAfterGetConnection(verticaSourceDTO, clearStatus));
        }
    }



    @Override
    protected String dealSql(ISourceDTO iSourceDTO, SqlQueryDTO sqlQueryDTO) {
        return String.format(TABLE_QUERY, sqlQueryDTO.getTableName());
    }

    @Override
    protected String getTableBySchemaSql(ISourceDTO sourceDTO, SqlQueryDTO queryDTO) {
        return String.format(SHOW_TABLES_SCHEMA, queryDTO.getSchema());
    }

    @Override
    public List<ColumnMetaDTO> getPartitionColumn(ISourceDTO source, SqlQueryDTO queryDTO) {
        throw new DtLoaderException("Not Support");
    }

    @Override
    public String getShowDbSql() {
        return SHOW_SCHEMA;
    }

    @Override
    protected String getCurrentDbSql() {
        throw new DtLoaderException("Not Support");
    }

    /**
     * 处理表名的展示
     *
     * @param schema
     * @param tableName
     * @return
     */
    private String transferSchemaAndTable(String schema, String tableName) {
        return String.format("%s.\"%s\"", schema, tableName);
    }

    /**
     * 从表获取表
     *
     * @param table
     * @return
     */
    private String getTableFromTable(String table) {
        if (StringUtils.isBlank(table)) {
            throw new DtLoaderException("table name cannot be empty");
        }

        if (table.contains("\"")) {
            return table.substring(table.indexOf("\"")).replaceAll("\"", "");
        }

        return null;
    }

    /**
     * 从表获取 Schema
     *
     * @param table
     * @return
     */
    private String getSchemaFromTable(String table) {
        if (StringUtils.isBlank(table)) {
            throw new DtLoaderException("table name cannot be empty");
        }

        if (table.contains("\"")) {
            return table.substring(0, table.indexOf("\"") - 1);
        }

        return null;
    }

    @Override
    protected ConnFactory getConnFactory() {
        return new VerticaConnFactory();
    }

    @Override
    protected DataSourceType getSourceType() {
        return DataSourceType.VERTICA;
    }

    @Override
    protected String getVersionSql() {
        return SHOW_VERSION;
    }
}
