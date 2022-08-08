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

package com.dtstack.dtcenter.common.loader.tidb.metadata;

import com.dtstack.dtcenter.common.loader.common.utils.DateUtil;
import com.dtstack.dtcenter.common.loader.mysql5.metadata.MysqlMetadataCollect;
import com.dtstack.dtcenter.common.loader.rdbms.ConnFactory;
import com.dtstack.dtcenter.common.loader.tidb.TidbConnFactory;
import com.dtstack.dtcenter.common.loader.tidb.metadata.constants.TidbMetadataCons;
import com.dtstack.dtcenter.loader.dto.metadata.entity.mysql.MysqlColumnEntity;
import com.dtstack.dtcenter.loader.dto.metadata.entity.mysql.MysqlTableEntity;
import com.dtstack.dtcenter.loader.dto.metadata.entity.rdb.ColumnEntity;
import com.dtstack.dtcenter.loader.dto.metadata.entity.rdb.MetadataRdbEntity;
import com.dtstack.dtcenter.loader.dto.metadata.entity.tidb.MetadataTidbEntity;
import com.dtstack.dtcenter.loader.dto.metadata.entity.tidb.TidbPartitionEntity;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class TidbMetaDataCollect extends MysqlMetadataCollect {

    @Override
    protected MetadataRdbEntity createMetadataRdbEntity() {
        try {
            return queryMetaData();
        } catch (SQLException e) {
            throw new DtLoaderException("sql exception");
        }
    }

    /**
     * 查询元数据信息
     *
     * @return
     * @throws SQLException
     */
    protected MetadataTidbEntity queryMetaData() throws SQLException {
        MetadataTidbEntity metadataTidbEntity = new MetadataTidbEntity();
        MysqlTableEntity tableProp = queryTableProp();
        List<MysqlColumnEntity> columns = queryColumn(currentDatabase);
        List<TidbPartitionEntity> partitions = queryPartition();
        Map<String, String> updateTime = queryAddPartition();
        List<String> partitionColumns = queryPartitionColumn();
        List<MysqlColumnEntity> partitionsColumnEntities = new ArrayList<>();
        columns.removeIf((MysqlColumnEntity perColumn) -> {
            for (String partitionColumn : partitionColumns) {
                if (StringUtils.equals(partitionColumn, perColumn.getName())) {
                    //copy属性值
                    partitionsColumnEntities.add(perColumn);
                    return true;
                }
            }
            return false;
        });
        metadataTidbEntity.setTableProperties(tableProp);
        metadataTidbEntity.setColumns(columns);
        if (CollectionUtils.size(partitions) > 1) {
            for (TidbPartitionEntity tidbPartitionEntity : partitions) {
                String columnName = tidbPartitionEntity.getColumnName();
                tidbPartitionEntity.setCreateTime(MapUtils.getString(updateTime, columnName) == null ? updateTime.get(TidbMetadataCons.KEY_UPDATE_TIME) : MapUtils.getString(updateTime, columnName));
            }
            metadataTidbEntity.setPartitions(partitions);
        }
        metadataTidbEntity.setPartitionColumns(partitionsColumnEntities);
        return metadataTidbEntity;
    }

    /**
     * 查看表的元数据信息
     *
     * @return
     * @throws SQLException
     */
    public MysqlTableEntity queryTableProp() throws SQLException {
        String tableName = (String) currentObject;
        MysqlTableEntity mysqlTableEntity = new MysqlTableEntity();
        String sql = String.format(TidbMetadataCons.SQL_QUERY_TABLE_INFO, tableName);
        try (ResultSet rs = statement.executeQuery(sql)) {
            while (rs.next()) {
                mysqlTableEntity.setRows(Long.valueOf(rs.getString(TidbMetadataCons.RESULT_ROWS)));
                mysqlTableEntity.setTotalSize(Long.valueOf(rs.getString(TidbMetadataCons.RESULT_DATA_LENGTH)));
                mysqlTableEntity.setCreateTime(DateUtil.stringToDate(rs.getString(TidbMetadataCons.RESULT_CREATE_TIME)).getTime());
                mysqlTableEntity.setComment(rs.getString(TidbMetadataCons.RESULT_COMMENT));
            }
        }
        return mysqlTableEntity;
    }

    @Override
    public List<MysqlColumnEntity> queryColumn(String schema){
        String tableName = (String) currentObject;
        List<MysqlColumnEntity> columns = new LinkedList<>();
        String sql = String.format(TidbMetadataCons.SQL_QUERY_COLUMN, tableName, schema);
        try {
            try (ResultSet rs = statement.executeQuery(sql)) {
                while (rs.next()) {
                    MysqlColumnEntity columnEntity = new MysqlColumnEntity();
                    columnEntity.setName(rs.getString(TidbMetadataCons.RESULT_COLUMN_NAME));
                    String type = rs.getString(TidbMetadataCons.RESULT_COLUMN_TYPE);
                    columnEntity.setType(type);
                    columnEntity.setNullAble(StringUtils.equals(rs.getString(TidbMetadataCons.RESULT_COLUMN_NULL), TidbMetadataCons.KEY_TES) ? TidbMetadataCons.KEY_TRUE : TidbMetadataCons.KEY_FALSE);
                    columnEntity.setPrimaryKey(StringUtils.equals(rs.getString(TidbMetadataCons.RESULT_COLUMN_PRIMARY_KEY), TidbMetadataCons.KEY_PRI) ? TidbMetadataCons.KEY_TRUE : TidbMetadataCons.KEY_FALSE);
                    columnEntity.setDefaultValue(rs.getString(TidbMetadataCons.RESULT_COLUMN_DEFAULT));
                    String length = StringUtils.contains(type, '(') ? StringUtils.substring(type, type.indexOf("(") + 1, !type.contains(",") ? type.indexOf(")"): type.indexOf(",")) : "0";
                    columnEntity.setLength(Integer.valueOf(length));
                    columnEntity.setComment(rs.getString(TidbMetadataCons.RESULT_COLUMN_COMMENT));
                    columnEntity.setIndex(rs.getInt(TidbMetadataCons.RESULT_COLUMN_POSITION));
                    setPrecisionAndDigital(schema, rs, columnEntity);
                    columns.add(columnEntity);
                }
            }
        } catch (SQLException e) {
            throw new DtLoaderException("sql exception");
        }
        return columns;
    }

    /**
     * 查看分区的元数据信息
     *
     * @return
     * @throws SQLException
     */
    protected List<TidbPartitionEntity> queryPartition() throws SQLException {
        String tableName = (String) currentObject;
        List<TidbPartitionEntity> partitions = new LinkedList<>();
        String sql = String.format(TidbMetadataCons.SQL_QUERY_PARTITION, tableName);
        try (ResultSet rs = statement.executeQuery(sql)) {
            while (rs.next()) {
                TidbPartitionEntity tidbPartitionEntity = new TidbPartitionEntity();
                tidbPartitionEntity.setColumnName(rs.getString(TidbMetadataCons.RESULT_PARTITION_NAME));
                tidbPartitionEntity.setCreateTime(rs.getString(TidbMetadataCons.RESULT_PARTITION_CREATE_TIME));
                tidbPartitionEntity.setPartitionRows(rs.getLong(TidbMetadataCons.RESULT_PARTITION_TABLE_ROWS));
                tidbPartitionEntity.setPartitionSize(rs.getLong(TidbMetadataCons.RESULT_PARTITION_DATA_LENGTH));
                partitions.add(tidbPartitionEntity);
            }
        }
        return partitions;
    }

    /**
     * 查看分区的创建时间
     *
     * @return
     * @throws SQLException
     */
    protected Map<String, String> queryAddPartition() throws SQLException {
        String tableName = (String) currentObject;
        Map<String, String> result = new HashMap<>(16);
        String sql = String.format(TidbMetadataCons.SQL_QUERY_UPDATE_TIME, tableName);
        try (ResultSet rs = statement.executeQuery(sql)) {
            while (rs.next()) {
                /* 考虑partitionName 为空的情况 */
                String name = rs.getString(TidbMetadataCons.RESULT_PARTITIONNAME);
                if (StringUtils.isNotBlank(name)) {
                    result.put(name, rs.getString(TidbMetadataCons.RESULT_UPDATE_TIME));
                } else {
                    result.put(TidbMetadataCons.KEY_UPDATE_TIME, rs.getString(TidbMetadataCons.RESULT_UPDATE_TIME));
                }
            }
        }
        return result;
    }

    /**
     * 查询分区字段集合
     *
     * @return
     * @throws SQLException
     */
    protected List<String> queryPartitionColumn() throws SQLException {
        String tableName = (String) currentObject;
        List<String> partitionColumns = new LinkedList<>();
        String sql = String.format(TidbMetadataCons.SQL_QUERY_PARTITION_COLUMN, tableName);
        try (ResultSet rs = statement.executeQuery(sql)) {
            while (rs.next()) {
                String partitionExp = rs.getString(TidbMetadataCons.RESULT_PARTITION_EXPRESSION);
                if (StringUtils.isNotBlank(partitionExp)) {
                    String columnName = partitionExp.substring(partitionExp.indexOf("`") + 1, partitionExp.lastIndexOf("`"));
                    partitionColumns.add(columnName);
                }
            }
        }
        return partitionColumns;
    }

    @Override
    protected void setPrecisionAndDigital(String schema, ResultSet resultSet, ColumnEntity columnEntity) throws SQLException {
        Object digits = resultSet.getObject(TidbMetadataCons.RESULT_COLUMN_SCALE);
        if (digits != null) {
            columnEntity.setDigital(resultSet.getInt(TidbMetadataCons.RESULT_COLUMN_SCALE));
        }
        Object precision = resultSet.getObject(TidbMetadataCons.RESULT_COLUMN_PRECISION);
        if (precision != null) {
            columnEntity.setPrecision(resultSet.getInt(TidbMetadataCons.RESULT_COLUMN_PRECISION));
        }
    }


    @Override
    public List<Object> showTables(){
        List<Object> tables = new ArrayList<>();
        try (ResultSet rs = statement.executeQuery(TidbMetadataCons.SQL_SHOW_TABLES)) {
            while (rs.next()) {
                tables.add(rs.getString(1));
            }
        } catch (SQLException throwables) {
            throw new DtLoaderException("sql exception");
        }
        return tables;
    }

    @Override
    protected ConnFactory getConnFactory() {
        return new TidbConnFactory();
    }
}
