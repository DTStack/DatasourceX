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

package com.dtstack.dtcenter.common.loader.postgresql.metadata;

import com.dtstack.dtcenter.common.loader.postgresql.PostgresqlConnFactory;
import com.dtstack.dtcenter.common.loader.postgresql.metadata.constants.PostgresqlCons;
import com.dtstack.dtcenter.common.loader.postgresql.metadata.utils.CommonUtils;
import com.dtstack.dtcenter.common.loader.rdbms.ConnFactory;
import com.dtstack.dtcenter.common.loader.rdbms.metadata.RdbmsMetaDataCollect;
import com.dtstack.dtcenter.common.loader.rdbms.metadata.constants.RdbCons;
import com.dtstack.dtcenter.loader.dto.metadata.entity.MetadataEntity;
import com.dtstack.dtcenter.loader.dto.metadata.entity.postgresql.IndexMetaData;
import com.dtstack.dtcenter.loader.dto.metadata.entity.postgresql.MetadataPostgresqlEntity;
import com.dtstack.dtcenter.loader.dto.metadata.entity.postgresql.TableMetaData;
import com.dtstack.dtcenter.loader.dto.metadata.entity.rdb.ColumnEntity;
import com.dtstack.dtcenter.loader.dto.metadata.entity.rdb.MetadataRdbEntity;
import com.dtstack.dtcenter.loader.dto.source.PostgresqlSourceDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.dtstack.dtcenter.common.loader.metadata.constants.BaseCons.DEFAULT_OPERA_TYPE;
import static com.dtstack.dtcenter.common.loader.postgresql.metadata.constants.PostgresqlCons.COLUMN_NAME;
import static com.dtstack.dtcenter.common.loader.postgresql.metadata.constants.PostgresqlCons.COUNT;
import static com.dtstack.dtcenter.common.loader.postgresql.metadata.constants.PostgresqlCons.INDEXDEF;
import static com.dtstack.dtcenter.common.loader.postgresql.metadata.constants.PostgresqlCons.INDEXNAME;
import static com.dtstack.dtcenter.common.loader.postgresql.metadata.constants.PostgresqlCons.INDEX_NAME;
import static com.dtstack.dtcenter.common.loader.postgresql.metadata.constants.PostgresqlCons.KEY_TABLE_NAME;
import static com.dtstack.dtcenter.common.loader.postgresql.metadata.constants.PostgresqlCons.SIZE;
import static com.dtstack.dtcenter.common.loader.postgresql.metadata.constants.PostgresqlCons.TABLE_NAME;
import static com.dtstack.dtcenter.common.loader.postgresql.metadata.constants.PostgresqlCons.TABLE_SCHEMA;

/**
 * postgresql 元数据采集器
 *
 * @author by zhiyi
 * @date 2022/4/7 3:49 下午
 */
@Slf4j
public class PostgresqlMetadataCollect extends RdbmsMetaDataCollect {

    /**
     * 是否设置了搜索路径
     */
    private boolean isChanged = false;

    /**
     * schema
     */
    private String schemaName;

    /**
     * table
     */
    private String tableName;

    @Override
    protected void openConnection() {
        try {
            connection = getConnectionForCurrentDataBase();
            statement = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
            if (CollectionUtils.isEmpty(tableList)) {
                tableList = showTables();
            }
        } catch (Exception e) {
            log.error("openConnection error", e);
        }
    }

    @Override
    protected MetadataRdbEntity createMetadataRdbEntity() {
        MetadataPostgresqlEntity postgresqlEntity = null;
        try {
            postgresqlEntity = new MetadataPostgresqlEntity();
            postgresqlEntity.setColumns(queryColumn(schemaName));
            TableMetaData tableMetaData = new TableMetaData();
            tableMetaData.setTableName(tableName);
            tableMetaData.setRows(showTableDataCount());
            tableMetaData.setIndexes(showIndexes());
            tableMetaData.setPrimaryKey(showTablePrimaryKey());
            tableMetaData.setTotalSize(showTableSize());
            postgresqlEntity.setTableProperties(tableMetaData);
        } catch (Exception e) {
            log.error("createMetadataRdbEntity error", e);
        }
        return postgresqlEntity;
    }

    @Override
    protected List<? extends ColumnEntity> queryColumn(String schema) {
        List<ColumnEntity> columnEntities = Lists.newArrayList();
        Map<String, String> table = (Map<String, String>) currentObject;
        String currentTable = table.get(KEY_TABLE_NAME);
        try (ResultSet resultSet = connection.getMetaData().getColumns(currentDatabase, schema, currentTable, null);
             ResultSet primaryResultSet = connection.getMetaData().getPrimaryKeys(currentDatabase, schema, currentTable)) {
            Set<String> primaryColumns = Sets.newHashSet();
            while (primaryResultSet.next()) {
                primaryColumns.add(primaryResultSet.getString(RdbCons.RESULT_COLUMN_NAME));
            }
            while (resultSet.next()) {
                ColumnEntity columnEntity = new ColumnEntity();
                columnEntity.setName(resultSet.getString(RdbCons.RESULT_COLUMN_NAME));
                columnEntity.setType(resultSet.getString(RdbCons.RESULT_TYPE_NAME));
                columnEntity.setIndex(resultSet.getInt(RdbCons.RESULT_ORDINAL_POSITION));
                columnEntity.setDefaultValue(resultSet.getString(RdbCons.RESULT_COLUMN_DEF));
                columnEntity.setNullAble(StringUtils.equals(resultSet.getString(RdbCons.RESULT_IS_NULLABLE), RdbCons.KEY_TES) ? RdbCons.KEY_TRUE : RdbCons.KEY_FALSE);
                columnEntity.setComment(resultSet.getString(RdbCons.RESULT_REMARKS));
                columnEntity.setLength(resultSet.getInt(RdbCons.RESULT_COLUMN_SIZE));
                columnEntity.setPrimaryKey(primaryColumns.contains(columnEntity.getName()) ? RdbCons.KEY_TRUE : RdbCons.KEY_FALSE);
                setPrecisionAndDigital(schema, resultSet, columnEntity);
                columnEntities.add(columnEntity);
            }
        } catch (SQLException e) {
            log.error("queryColumn failed", e);
            throw new DtLoaderException("execute sql error", e);
        }
        return columnEntities;
    }

    @Override
    public MetadataEntity readNext() {
        MetadataPostgresqlEntity metadataPostgresqlEntity = new MetadataPostgresqlEntity();
        currentObject = iterator.next();
        Map<String, String> map = (Map<String, String>) currentObject;
        //保证在同一schema下只需要设置一次搜索路径
        if (schemaName != null && !map.get(PostgresqlCons.KEY_SCHEMA_NAME).equals(schemaName)) {
            isChanged = false;
        }
        schemaName = map.get(PostgresqlCons.KEY_SCHEMA_NAME);
        tableName = map.get(KEY_TABLE_NAME);
        try {
            metadataPostgresqlEntity = (MetadataPostgresqlEntity) createMetadataRdbEntity();
            metadataPostgresqlEntity.setDataBaseName(currentDatabase);
            metadataPostgresqlEntity.setSchema(schemaName);
            metadataPostgresqlEntity.setTableName(tableName);
            metadataPostgresqlEntity.setQuerySuccess(true);
        } catch (Exception e) {
            metadataPostgresqlEntity.setQuerySuccess(false);
            metadataPostgresqlEntity.setErrorMsg(e.getMessage());
            throw new RuntimeException(e);
        }
        metadataPostgresqlEntity.setOperaType(DEFAULT_OPERA_TYPE);
        return metadataPostgresqlEntity;
    }

    @Override
    protected ConnFactory getConnFactory() {
        return new PostgresqlConnFactory();
    }

    /**
     * 查询当前database中所有表名
     *
     * @return List<Object>
     **/
    @Override
    public List<Object> showTables() {
        List<Object> tables = new ArrayList<>();
        try (ResultSet resultSet = statement.executeQuery(PostgresqlCons.SQL_SHOW_TABLES)) {
            while (resultSet.next()) {
                HashMap<String, String> map = new HashMap<>(8);
                map.put(PostgresqlCons.KEY_SCHEMA_NAME, resultSet.getString(TABLE_SCHEMA));
                map.put(KEY_TABLE_NAME, resultSet.getString(TABLE_NAME));
                tables.add(map);
            }
        } catch (SQLException e) {
            log.error("failed to query table, currentDb = {} ", currentDatabase, e);
        }
        return tables;
    }

    /**
     * 查询表所占磁盘空间
     *
     * @return long
     **/
    private long showTableSize() throws SQLException {
        long size = 0;
        String sql = String.format(PostgresqlCons.SQL_SHOW_TABLE_SIZE, schemaName, tableName);
        try (ResultSet resultSet = statement.executeQuery(sql)) {
            if (resultSet.next()) {
                size = resultSet.getLong(SIZE);
            }
        }
        return size;
    }

    /**
     * 查询表中的主键名
     *
     * @return String
     **/
    private List<String> showTablePrimaryKey() throws SQLException {
        List<String> primaryKey = new ArrayList<>();
        ResultSet resultSet = connection.getMetaData().getPrimaryKeys(currentDatabase, schemaName, tableName);
        while (resultSet.next()) {
            primaryKey.add(resultSet.getString(COLUMN_NAME));
        }
        return primaryKey;
    }

    /**
     * 查询表中有多少条数据
     *
     * @return long
     **/
    private long showTableDataCount() throws SQLException {
        long dataCount = 0;
        String sql = String.format(PostgresqlCons.SQL_SHOW_COUNT, tableName);
        //由于主键所在系统表不具备schema隔离性，所以在查询前需要设置查询路径为当前schema
        if (!isChanged) {
            setSearchPath();
        }
        try (ResultSet countSet = statement.executeQuery(sql)) {
            if (countSet.next()) {
                dataCount = countSet.getLong(COUNT);
            }
        }
        return dataCount;
    }

    /**
     * 查询表中索引名
     *
     * @return List<String>
     **/
    private List<IndexMetaData> showIndexes() throws SQLException {
        List<IndexMetaData> result = new ArrayList<>();
        //索引名对columnName的映射
        HashMap<String, ArrayList<String>> indexColumns = new HashMap<>(16);
        //索引名对索引类型的映射
        HashMap<String, String> indexType = new HashMap<>(16);

        ResultSet resultSet = connection.getMetaData().getIndexInfo(currentDatabase, schemaName, tableName, false, false);
        while (resultSet.next()) {
            ArrayList<String> columns = indexColumns.get(resultSet.getString(INDEX_NAME));
            if (columns != null) {
                columns.add(resultSet.getString(COLUMN_NAME));
            } else {
                ArrayList<String> list = new ArrayList<>();
                list.add(resultSet.getString(COLUMN_NAME));
                indexColumns.put(resultSet.getString(INDEX_NAME), list);
            }
        }

        String sql = String.format(PostgresqlCons.SQL_SHOW_INDEX, schemaName, tableName);
        ResultSet indexResultSet = statement.executeQuery(sql);
        while (indexResultSet.next()) {
            indexType.put(indexResultSet.getString(INDEXNAME)
                    , CommonUtils.indexType(indexResultSet.getString(INDEXDEF)));
        }

        for (String key : indexColumns.keySet()) {
            result.add(new IndexMetaData(key, indexColumns.get(key), indexType.get(key)));
        }

        return result;
    }

    /**
     * 设置查询路径为当前schema
     **/
    private void setSearchPath() throws SQLException {
        String sql = String.format(PostgresqlCons.SQL_SET_SEARCHPATH, schemaName);
        statement.execute(sql);
        //置为true表示查询路径已经为当前schema
        isChanged = true;
    }

    /**
     * 由于postgresql没有类似于MySQL的"use database"的SQL语句，所以切换数据库需要重新建立连接
     *
     * @return Connection
     **/
    private Connection getConnectionForCurrentDataBase() throws Exception {
        PostgresqlSourceDTO sourceDTO = (PostgresqlSourceDTO) this.sourceDTO;
        String url = CommonUtils.dbUrlTransform(sourceDTO.getUrl(), currentDatabase);
        sourceDTO.setUrl(url);
        return connFactory.getConn(sourceDTO);
    }
}
