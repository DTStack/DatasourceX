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

package com.dtstack.dtcenter.common.loader.mysql.sharding;

import com.dtstack.dtcenter.common.loader.common.utils.DBUtil;
import com.dtstack.dtcenter.common.loader.common.utils.SchemaUtil;
import com.dtstack.dtcenter.common.loader.common.utils.SearchUtil;
import com.dtstack.dtcenter.common.loader.rdbms.AbsRdbmsClient;
import com.dtstack.dtcenter.common.loader.rdbms.ConnFactory;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;
import com.dtstack.dtcenter.loader.dto.source.MysqlShardingSourceDTO;
import com.dtstack.dtcenter.loader.dto.source.RdbmsSourceDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import com.dtstack.dtcenter.loader.client.IDownloader;
import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * sharding-proxy for mysql
 *
 * @author luming
 * @date 2022/1/10
 */
@Slf4j
public class MysqlShardingClient extends AbsRdbmsClient {

    // 选择当前使用的库
    private static final String CURRENT_DB = "select database()";
    // 获取当前库的所有表
    private static final String SCHEMA_TABLES = "show tables from %s";
    // 模糊查询数据库
    private static final String SHOW_DB_LIKE = "show databases like '%s'";
    // 获取当前版本号
    private static final String SHOW_VERSION = "select version()";
    // 不存在
    private static final String DONT_EXIST = "doesn't exist";
    // 获取建表语句
    private static final String SHOW_CREATE_TABLE_SQL = "SHOW CREATE TABLE %s";
    // 创建数据库
    private static final String CREATE_SCHEMA_SQL_TMPL = "create schema %s ";
    // 查询表获取的表名称key前缀，sharding-proxy会自动拼接后面的db名
    private static final String TABLE_NAME_KEY = "Tables_in";
    // 查询表获取的表类型key
    private static final String TABLE_TYPE_KEY = "Table_type";
    // 视图
    private static final String VIEW = "'VIEW'";
    // 普通表
    private static final String BASE_TABLE = "'BASE TABLE'";

    @Override
    protected ConnFactory getConnFactory() {
        return new MysqlShardingConnFactory();
    }

    @Override
    protected DataSourceType getSourceType() {
        return DataSourceType.MYSQL_SHARDING;
    }

    @Override
    public List<String> getTableListBySchema(ISourceDTO source, SqlQueryDTO queryDTO) {
        String schema = SchemaUtil.getSchema(source, queryDTO);
        // 如果不传scheme，默认使用当前连接使用的schema
        if (StringUtils.isBlank(schema)) {
            if (log.isDebugEnabled()) {
                log.debug("schema is empty，get current used schema!");
            }
            // 获取当前数据库
            try {
                schema = getCurrentDatabase(source);
                queryDTO.setSchema(schema);
            } catch (Exception e) {
                throw new DtLoaderException(String.format("get current used database error！,%s", e.getMessage()), e);
            }
        }

        queryDTO.setSql(getTableBySchemaSql(source, queryDTO));
        List<Map<String, Object>> allTables = executeQuery(source, queryDTO);
        if (CollectionUtils.isEmpty(allTables)) {
            return Lists.newArrayList();
        }

        return dealNamePatternAndView(allTables, queryDTO);
    }

    /**
     * 处理模糊查询和视图相关逻辑
     * sharding-proxy由于不能走information_schema表查询信息，只能先查出来所有表再过滤
     *
     * @param allTables
     * @param queryDTO
     * @return
     */
    private List<String> dealNamePatternAndView(List<Map<String, Object>> allTables, SqlQueryDTO queryDTO) {
        //不包含视图需要去除视图表
        if (!queryDTO.getView()) {
            allTables.removeIf(map -> Objects.equals(MapUtils.getString(map, TABLE_TYPE_KEY), VIEW));
        }

        //转换为只包含表名的list
        List<String> collect = allTables.stream()
                .map(map -> {
                    for (Map.Entry<String, Object> entry : map.entrySet()) {
                        if (entry.getKey().startsWith(TABLE_NAME_KEY)) {
                            return String.valueOf(entry.getValue());
                        }
                    }
                    return null;
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toList());

        String namePattern = queryDTO.getTableNamePattern();
        if (StringUtils.isBlank(namePattern)) {
            return collect;
        }

        return SearchUtil.handleSearchAndLimit(collect, queryDTO);
    }

    @Override
    protected String getCurrentDbSql() {
        return CURRENT_DB;
    }

    @Override
    protected String getTableBySchemaSql(ISourceDTO sourceDTO, SqlQueryDTO queryDTO) {
        return String.format(SCHEMA_TABLES, queryDTO.getSchema());
    }

    @Override
    public List<String> getTableList(ISourceDTO iSource, SqlQueryDTO queryDTO) {
        return getTableListBySchema(iSource, queryDTO);
    }

    @Override
    public IDownloader getDownloader(ISourceDTO source, SqlQueryDTO queryDTO) throws Exception {
        MysqlDownloader mysqlDownloader = new MysqlDownloader(
                getCon(source), queryDTO.getSql(), ((MysqlShardingSourceDTO) source).getSchema());
        mysqlDownloader.configure();
        return mysqlDownloader;
    }

    @Override
    public Boolean isDatabaseExists(ISourceDTO source, String dbName) {
        if (StringUtils.isBlank(dbName)) {
            throw new DtLoaderException("database name is not empty");
        }
        List result = executeQuery(source, SqlQueryDTO.builder().sql(String.format(SHOW_DB_LIKE, dbName)).build());

        return CollectionUtils.isNotEmpty(result);
    }

    @Override
    public Boolean isTableExistsInDatabase(ISourceDTO source, String tableName, String dbName) {
        if (StringUtils.isBlank(dbName)) {
            throw new DtLoaderException("database name can't be null");
        }
        if (StringUtils.isBlank(tableName)) {
            throw new DtLoaderException("tableName name can't be null");
        }

        List<String> tables = getTableListBySchema(source, SqlQueryDTO.builder().schema(dbName).build());
        if (CollectionUtils.isEmpty(tables)) {
            return false;
        }

        return tables.stream().map(String::toLowerCase).anyMatch(name -> tableName.toLowerCase().equals(name));
    }

    @Override
    protected String getVersionSql() {
        return SHOW_VERSION;
    }

    @Override
    public String getTableMetaComment(ISourceDTO iSource, SqlQueryDTO queryDTO) {
        if (StringUtils.isBlank(queryDTO.getTableName())) {
            throw new DtLoaderException("tableName is empty");
        }
        MysqlShardingSourceDTO sourceDTO = (MysqlShardingSourceDTO) iSource;
        queryDTO.setSql(String.format(SHOW_CREATE_TABLE_SQL, queryDTO.getTableName()));

        List<Map<String, Object>> list = executeQuery(sourceDTO, queryDTO);
        if (CollectionUtils.isEmpty(list)) {
            throw new DtLoaderException("table is not exists");
        }

        String createSql = MapUtils.getString(list.get(0), "Create Table");
        return parse(createSql);
    }

    public String parse(String sql) {
        if (StringUtils.isBlank(sql)) {
            return "";
        }
        String comment;
        int index = sql.indexOf("COMMENT='");
        if (index < 0) {
            return "";
        }
        comment = sql.substring(index + 9);
        comment = comment.substring(0, comment.length() - 1);
        comment = new String(comment.getBytes(StandardCharsets.UTF_8));
        return comment;
    }

    @Override
    protected Map<String, String> getColumnComments(RdbmsSourceDTO sourceDTO, SqlQueryDTO queryDTO) {
        Connection connection = getCon(sourceDTO);
        Statement statement = null;
        ResultSet rs = null;
        Map<String, String> columnComments = new HashMap<>();
        try {
            statement = connection.createStatement();
            String queryColumnCommentSql =
                    "show full columns from " + transferSchemaAndTableName(sourceDTO, queryDTO);
            rs = statement.executeQuery(queryColumnCommentSql);
            while (rs.next()) {
                String columnName = rs.getString("Field");
                String columnComment = rs.getString("Comment");
                columnComments.put(columnName, columnComment);
            }
        } catch (Exception e) {
            if (e.getMessage().contains(DONT_EXIST)) {
                throw new DtLoaderException(String.format(queryDTO.getTableName() + "table not exist,%s", e.getMessage()), e);
            } else {
                throw new DtLoaderException(String.format("Failed to get the comment information of the field of the table: %s. Please contact the DBA to check the database and table information.",
                        queryDTO.getTableName()), e);
            }
        } finally {
            DBUtil.closeDBResources(rs, statement, connection);
        }
        return columnComments;
    }

    @Override
    protected String doDealType(ResultSetMetaData rsMetaData, Integer los) throws SQLException {
        int columnType = rsMetaData.getColumnType(los + 1);
        // text,mediumtext,longtext的jdbc类型名都是varchar，需要区分。不同的编码下，最大存储长度也不同。考虑1，2，3，4字节的编码

        if (columnType != Types.LONGVARCHAR) {
            return super.doDealType(rsMetaData, los);
        }

        int precision = rsMetaData.getPrecision(los + 1);
        if (precision >= 16383 && precision <= 65535) {
            return "TEXT";
        }

        if (precision >= 4194303 && precision <= 16777215) {
            return "MEDIUMTEXT";
        }

        if (precision >= 536870911 && precision <= 2147483647) {
            return "LONGTEXT";
        }

        return super.doDealType(rsMetaData, los);
    }

    @Override
    protected String transferTableName(String tableName) {
        return tableName.contains("`") ? tableName : String.format("`%s`", tableName);
    }

    /**
     * 处理 schema和tableName，适配schema和tableName中有.的情况
     *
     * @param schema
     * @param tableName
     * @return
     */
    @Override
    protected String transferSchemaAndTableName(String schema, String tableName) {
        if (!tableName.startsWith("`") || !tableName.endsWith("`")) {
            tableName = String.format("`%s`", tableName);
        }
        if (StringUtils.isBlank(schema)) {
            return tableName;
        }
        if (!schema.startsWith("`") || !schema.endsWith("`")) {
            schema = String.format("`%s`", schema);
        }
        return String.format("%s.%s", schema, tableName);
    }

    @Override
    protected String getCreateDatabaseSql(String dbName, String comment) {
        return String.format(CREATE_SCHEMA_SQL_TMPL, dbName);
    }
}
