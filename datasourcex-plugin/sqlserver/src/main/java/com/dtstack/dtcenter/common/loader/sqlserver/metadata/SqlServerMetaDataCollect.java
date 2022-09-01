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

package com.dtstack.dtcenter.common.loader.sqlserver.metadata;

import com.dtstack.dtcenter.common.loader.common.utils.DateUtil;
import com.dtstack.dtcenter.common.loader.rdbms.ConnFactory;
import com.dtstack.dtcenter.common.loader.rdbms.metadata.RdbmsMetaDataCollect;
import com.dtstack.dtcenter.common.loader.rdbms.metadata.constants.RdbCons;
import com.dtstack.dtcenter.common.loader.sqlserver.SQLServerConnFactory;
import com.dtstack.dtcenter.common.loader.sqlserver.metadata.constants.SqlServerMetadataCons;
import com.dtstack.dtcenter.loader.dto.metadata.entity.MetadataEntity;
import com.dtstack.dtcenter.loader.dto.metadata.entity.rdb.ColumnEntity;
import com.dtstack.dtcenter.loader.dto.metadata.entity.rdb.MetadataRdbEntity;
import com.dtstack.dtcenter.loader.dto.metadata.entity.sqlserver.MetadatasqlserverEntity;
import com.dtstack.dtcenter.loader.dto.metadata.entity.sqlserver.SqlserverIndexEntity;
import com.dtstack.dtcenter.loader.dto.metadata.entity.sqlserver.SqlserverPartitionEntity;
import com.dtstack.dtcenter.loader.dto.metadata.entity.sqlserver.SqlserverTableEntity;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import org.apache.commons.lang3.StringUtils;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class SqlServerMetaDataCollect extends RdbmsMetaDataCollect {

    /**当前schema*/
    protected String schema;

    /**当前表*/
    protected String table;

    @Override
    protected MetadataRdbEntity createMetadataRdbEntity(){
        MetadatasqlserverEntity metadatasqlserverEntity = new MetadatasqlserverEntity();

        SqlserverTableEntity tableEntity = null;
        List<ColumnEntity> columns = null;
        String key = null;
        try {
            tableEntity = queryTableProp();
            tableEntity.setIndex(queryIndexes());
            tableEntity.setPrimaryKey(queryTablePrimaryKey());
            tableEntity.setPartition(queryPartition());

            columns = (List<ColumnEntity>) queryColumn(schema);
            key = queryPartitionColumn();
        } catch (SQLException e) {
            throw new DtLoaderException("sql exception");
        }
        List<ColumnEntity> partitionColumn = distinctPartitionColumn(columns, key);

        metadatasqlserverEntity.setColumns(columns);
        metadatasqlserverEntity.setPartitionColumns(partitionColumn);
        metadatasqlserverEntity.setTableProperties(tableEntity);
        return metadatasqlserverEntity;
    }

    private SqlserverTableEntity queryTableProp() throws SQLException {
        SqlserverTableEntity tableEntity = new SqlserverTableEntity();
        String sql = String.format(SqlServerMetadataCons.SQL_SHOW_TABLE_PROPERTIES, quote(table), quote(schema));
        try (ResultSet resultSet = statement.executeQuery(sql)) {
            while (resultSet.next()) {
                tableEntity.setCreateTime(DateUtil.stringToDate(resultSet.getString(1)).getTime());
                tableEntity.setRows(resultSet.getLong(2));
                tableEntity.setTotalSize(resultSet.getLong(3));
                tableEntity.setComment(resultSet.getString(4));
            }
        }
        tableEntity.setTableName(table);
        return tableEntity;
    }

    private List<SqlserverIndexEntity> queryIndexes() throws SQLException {
        List<SqlserverIndexEntity> result = new ArrayList<>();
        //索引名对columnName的映射
        HashMap<String, ArrayList<String>> indexColumns = new HashMap<>(16);
        //索引名对索引类型的映射
        HashMap<String, String> indexType = new HashMap<>(16);

        ResultSet resultSet = connection.getMetaData().getIndexInfo(currentDatabase, schema, table, false, false);
        while (resultSet.next()) {
            ArrayList<String> columns = indexColumns.get(resultSet.getString("INDEX_NAME"));
            if (columns != null) {
                columns.add(resultSet.getString("COLUMN_NAME"));
            } else if (resultSet.getString("COLUMN_NAME") != null) {
                ArrayList<String> list = new ArrayList<>();
                list.add(resultSet.getString("COLUMN_NAME"));
                indexColumns.put(resultSet.getString("INDEX_NAME"), list);
            }
        }

        String sql = String.format(SqlServerMetadataCons.SQL_SHOW_TABLE_INDEX, quote(table), quote(schema));
        try (ResultSet indexResultSet = statement.executeQuery(sql)) {
            while (indexResultSet.next()) {
                indexType.put(indexResultSet.getString(1)
                        , indexResultSet.getString(3));
            }
        }
        for (String key : indexColumns.keySet()) {
            result.add(new SqlserverIndexEntity(key, indexType.get(key), indexColumns.get(key)));
        }

        return result;
    }

    private List<String> queryTablePrimaryKey() throws SQLException {
        List<String> primaryKey = new ArrayList<>();
        ResultSet resultSet = connection.getMetaData().getPrimaryKeys(currentDatabase, schema, table);
        while (resultSet.next()) {
            primaryKey.add(resultSet.getString("COLUMN_NAME"));
        }
        return primaryKey;
    }

    private List<SqlserverPartitionEntity> queryPartition() throws SQLException {
        List<SqlserverPartitionEntity> partitions = new ArrayList<>();
        String sql = String.format(SqlServerMetadataCons.SQL_SHOW_PARTITION, quote(table), quote(schema));
        try (ResultSet resultSet = statement.executeQuery(sql)) {
            while (resultSet.next()) {
                SqlserverPartitionEntity partitionEntity = new SqlserverPartitionEntity();
                partitionEntity.setColumnName(resultSet.getString(1));
                partitionEntity.setRows(resultSet.getLong(2));
                partitionEntity.setCreateTime(resultSet.getString(3));
                partitionEntity.setFileGroupName(resultSet.getString(4));
            }
        }
        return partitions;
    }

    private String queryPartitionColumn() throws SQLException {
        String partitionKey = null;
        String sql = String.format(SqlServerMetadataCons.SQL_SHOW_PARTITION_COLUMN, quote(table), quote(schema));
        try (ResultSet resultSet = statement.executeQuery(sql)) {
            while (resultSet.next()) {
                partitionKey = resultSet.getString(1);
            }
        }
        return partitionKey;
    }

    @Override
    protected List<ColumnEntity> queryColumn(String schema){
        List<ColumnEntity> columnEntities = new ArrayList<>();
        String primaryKey = null;
        try {
            Map<String, Integer> columnLength = queryColumnLength(schema, table);
            try (ResultSet resultSet = connection.getMetaData().getColumns(currentDatabase, schema, table, null)) {
                while (resultSet.next()) {
                    ColumnEntity perColumn = new ColumnEntity();
                    perColumn.setName(resultSet.getString(RdbCons.RESULT_COLUMN_NAME));
                    perColumn.setType(resultSet.getString(RdbCons.RESULT_TYPE_NAME));
                    perColumn.setIndex(resultSet.getInt(RdbCons.RESULT_ORDINAL_POSITION));
                    perColumn.setDefaultValue(resultSet.getString(RdbCons.RESULT_COLUMN_DEF));
                    perColumn.setNullAble(StringUtils.equals(resultSet.getString(RdbCons.RESULT_IS_NULLABLE), RdbCons.KEY_TES) ? RdbCons.KEY_TRUE : RdbCons.KEY_FALSE);
                    perColumn.setComment(resultSet.getString(RdbCons.RESULT_REMARKS));
                    perColumn.setLength(columnLength.get(resultSet.getString(RdbCons.RESULT_COLUMN_NAME)));
                    setPrecisionAndDigital(schema, resultSet, perColumn);
                    columnEntities.add(perColumn);
                }
            }
            String sql = String.format(SqlServerMetadataCons.SQL_QUERY_PRIMARY_KEY, quote(table), quote(schema));
            primaryKey = null;
            try (ResultSet resultSet = statement.executeQuery(sql)) {
                while (resultSet.next()) {
                    primaryKey = resultSet.getString(1);
                }
            }
        } catch (SQLException throwables) {
            throw new DtLoaderException("sql exception");
        }
        for (ColumnEntity columnEntity : columnEntities) {
            columnEntity.setPrimaryKey(StringUtils.equals(columnEntity.getName(), primaryKey) ? RdbCons.KEY_TRUE : RdbCons.KEY_FALSE);
        }
        return columnEntities;
    }

    /**
     * query the column length, return the mapping of column name and length
     *
     * **/
    public Map<String, Integer> queryColumnLength(String schema, String table) throws SQLException {
        String tableName = StringUtils.isNotEmpty(schema) ? schema + "." + table : table;
        String sql = String.format(SqlServerMetadataCons.SQL_QUERY_COLUMN_LENGTH, quote(tableName), quote(tableName));
        Map<String, Integer> map;
        try (ResultSet resultSet =  statement.executeQuery(sql)){
            map = new HashMap<>();
            while (resultSet.next()){
                map.put(resultSet.getString("name"), resultSet.getInt("length"));
            }
        }
        return map;
    }

    @Override
    protected void setPrecisionAndDigital(String schema, ResultSet resultSet, ColumnEntity columnEntity) throws SQLException {
        Object digits = resultSet.getObject(RdbCons.RESULT_DECIMAL_DIGITS);
        if (digits != null) {
            columnEntity.setDigital(resultSet.getInt(RdbCons.RESULT_DECIMAL_DIGITS));
        }
        Object precision = resultSet.getObject(RdbCons.RESULT_COLUMN_SIZE);
        if (precision != null) {
            columnEntity.setPrecision(resultSet.getInt(RdbCons.RESULT_COLUMN_SIZE));
        }
    }

    /**
     * 区分分区字段和非分区字段
     *
     * @param columns 所有字段
     * @param key     分区字段
     */
    private List<ColumnEntity> distinctPartitionColumn(List<ColumnEntity> columns, String key) {
        List<ColumnEntity> partitionColumn = new ArrayList<>();
        if (StringUtils.isNotEmpty(key)) {
            columns.removeIf(columnEntity ->
            {
                if (StringUtils.equals(key, columnEntity.getName())) {
                    partitionColumn.add(columnEntity);
                    return true;
                } else {
                    return false;
                }
            });
        }
        return partitionColumn;
    }

    private String quote(String name) {
        return "'" + name + "'";
    }

    @Override
    protected ConnFactory getConnFactory() {
        return new SQLServerConnFactory();
    }

    @Override
    public void switchDataBase() throws SQLException {
        // database 以数字开头时，需要双引号
        statement.execute(String.format(SqlServerMetadataCons.SQL_SWITCH_schema, currentDatabase));
    }

    @Override
    public MetadataEntity readNext() {
        currentObject = iterator.next();
        Map<String, String> map = (Map<String, String>) currentObject;
        schema = map.get(SqlServerMetadataCons.KEY_SCHEMA_NAME);
        table = map.get(SqlServerMetadataCons.KEY_TABLE_NAME);
        currentObject = table;

        if(!filterTable(currentObject, metadataCollectCondition.getExcludeDatabase(), metadataCollectCondition.getExcludeTable())){
            MetadatasqlserverEntity metadatasqlserverEntity = new MetadatasqlserverEntity();


            try {
                metadatasqlserverEntity = (MetadatasqlserverEntity) createMetadataRdbEntity();
                metadatasqlserverEntity.setDatabaseName(currentDatabase);
                metadatasqlserverEntity.setSchema(schema);
                metadatasqlserverEntity.setTableName(table);
                metadatasqlserverEntity.setQuerySuccess(true);

            } catch (Exception e) {
                metadatasqlserverEntity.setQuerySuccess(false);
                metadatasqlserverEntity.setErrorMsg("exception");
                throw new RuntimeException(e);
            }
            return metadatasqlserverEntity;
        }
        return null;
    }

    @Override
    public List<Object> showTables() {
        List<Object> tableNameList = new LinkedList<>();
        try (ResultSet rs = statement.executeQuery(SqlServerMetadataCons.SQL_SHOW_TABLES)) {
            while (rs.next()) {
                HashMap<String, String> map = new HashMap<>();
                map.put(SqlServerMetadataCons.KEY_SCHEMA_NAME, rs.getString(1));
                map.put(SqlServerMetadataCons.KEY_TABLE_NAME, rs.getString(2));
                tableNameList.add(map);
            }
        } catch (SQLException e) {
           throw new DtLoaderException("sql exception");
        }
        return tableNameList;
    }
}
