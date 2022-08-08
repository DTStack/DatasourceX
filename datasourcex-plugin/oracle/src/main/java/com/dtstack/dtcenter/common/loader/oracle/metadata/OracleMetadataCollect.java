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

package com.dtstack.dtcenter.common.loader.oracle.metadata;

import com.dtstack.dtcenter.common.loader.common.utils.DateUtil;
import com.dtstack.dtcenter.common.loader.oracle.OracleConnFactory;
import com.dtstack.dtcenter.common.loader.rdbms.ConnFactory;
import com.dtstack.dtcenter.common.loader.rdbms.metadata.RdbmsMetaDataCollect;
import com.dtstack.dtcenter.loader.dto.metadata.entity.oracle.MetadataOracleEntity;
import com.dtstack.dtcenter.loader.dto.metadata.entity.oracle.OracleColumnEntity;
import com.dtstack.dtcenter.loader.dto.metadata.entity.oracle.OracleIndexEntity;
import com.dtstack.dtcenter.loader.dto.metadata.entity.oracle.OracleTableEntity;
import com.dtstack.dtcenter.loader.dto.metadata.entity.rdb.MetadataRdbEntity;
import com.google.common.collect.Maps;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static com.dtstack.dtcenter.common.loader.oracle.metadata.constants.OracleMetaDataCons.COMMA_SYMBOL;
import static com.dtstack.dtcenter.common.loader.oracle.metadata.constants.OracleMetaDataCons.KEY_CREATE_TIME;
import static com.dtstack.dtcenter.common.loader.oracle.metadata.constants.OracleMetaDataCons.KEY_MAX_NUMBER;
import static com.dtstack.dtcenter.common.loader.oracle.metadata.constants.OracleMetaDataCons.KEY_NUMBER;
import static com.dtstack.dtcenter.common.loader.oracle.metadata.constants.OracleMetaDataCons.KEY_PARTITION_KEY;
import static com.dtstack.dtcenter.common.loader.oracle.metadata.constants.OracleMetaDataCons.KEY_PRIMARY_KEY;
import static com.dtstack.dtcenter.common.loader.oracle.metadata.constants.OracleMetaDataCons.MAX_TABLE_SIZE;
import static com.dtstack.dtcenter.common.loader.oracle.metadata.constants.OracleMetaDataCons.NUMBER_PRECISION;
import static com.dtstack.dtcenter.common.loader.oracle.metadata.constants.OracleMetaDataCons.SQL_PARTITION_KEY;
import static com.dtstack.dtcenter.common.loader.oracle.metadata.constants.OracleMetaDataCons.SQL_QUERY_COLUMN;
import static com.dtstack.dtcenter.common.loader.oracle.metadata.constants.OracleMetaDataCons.SQL_QUERY_COLUMN_TOTAL;
import static com.dtstack.dtcenter.common.loader.oracle.metadata.constants.OracleMetaDataCons.SQL_QUERY_INDEX;
import static com.dtstack.dtcenter.common.loader.oracle.metadata.constants.OracleMetaDataCons.SQL_QUERY_INDEX_TOTAL;
import static com.dtstack.dtcenter.common.loader.oracle.metadata.constants.OracleMetaDataCons.SQL_QUERY_PRIMARY_KEY;
import static com.dtstack.dtcenter.common.loader.oracle.metadata.constants.OracleMetaDataCons.SQL_QUERY_PRIMARY_KEY_TOTAL;
import static com.dtstack.dtcenter.common.loader.oracle.metadata.constants.OracleMetaDataCons.SQL_QUERY_TABLE_CREATE_TIME;
import static com.dtstack.dtcenter.common.loader.oracle.metadata.constants.OracleMetaDataCons.SQL_QUERY_TABLE_CREATE_TIME_TOTAL;
import static com.dtstack.dtcenter.common.loader.oracle.metadata.constants.OracleMetaDataCons.SQL_QUERY_TABLE_PARTITION_KEY;
import static com.dtstack.dtcenter.common.loader.oracle.metadata.constants.OracleMetaDataCons.SQL_QUERY_TABLE_PROPERTIES;
import static com.dtstack.dtcenter.common.loader.oracle.metadata.constants.OracleMetaDataCons.SQL_QUERY_TABLE_PROPERTIES_TOTAL;
import static com.dtstack.dtcenter.common.loader.oracle.metadata.constants.OracleMetaDataCons.SQL_SHOW_TABLES;
import static com.dtstack.dtcenter.common.loader.rdbms.metadata.constants.RdbCons.KEY_FALSE;
import static com.dtstack.dtcenter.common.loader.rdbms.metadata.constants.RdbCons.KEY_TRUE;

/**
 * oracle元数据拾取
 *
 * @author by zhiyi
 * @date 2022/4/7 2:42 下午
 */
@Slf4j
public class OracleMetadataCollect extends RdbmsMetaDataCollect {

    /**
     * 表基本属性
     */
    private Map<String, OracleTableEntity> tablePropertiesMap;

    /**
     * 列基本属性
     */
    private Map<String, List<OracleColumnEntity>> columnListMap;

    /**
     * 索引基本属性
     */
    private Map<String, List<OracleIndexEntity>> indexListMap;

    /**
     * 主键信息
     */
    private Map<String, String> primaryKeyMap;

    /**
     * 表创建时间
     */
    private Map<String, String> createdTimeMap;

    /**
     * 分区列
     */
    private Map<String, String> partitionMap;

    private String allTable;

    private String sql;

    private Integer currentTablePosition = 0;

    @Override
    protected MetadataRdbEntity createMetadataRdbEntity() {
        try {
            return queryMetaData((String) currentObject);
        } catch (SQLException throwable) {
            log.error("queryMetaData error, currentObject:{}, error msg:{}", currentObject, throwable.getMessage());
        }
        return null;
    }

    @Override
    protected ConnFactory getConnFactory() {
        return new OracleConnFactory();
    }

    @Override
    public List<Object> showTables() {
        List<Object> tableNameList = new LinkedList<>();
        sql = String.format(SQL_SHOW_TABLES, quote(currentDatabase));
        try (ResultSet rs = statement.executeQuery(sql)) {
            while (rs.next()) {
                tableNameList.add(rs.getString(1));
            }
        } catch (SQLException throwable) {
            log.error("executeQuery error, sql:{}, error msg:{}", sql, throwable.getMessage());
        }
        return tableNameList;
    }


    /**
     * 转义
     */
    protected String quote(String name) {
        return String.format("'%s'", name);
    }


    /**
     * 查询表元数据
     *
     * @param tableName 表名
     * @return 元数据信息
     * @throws SQLException sql执行异常
     */
    protected MetadataOracleEntity queryMetaData(String tableName) throws SQLException {
        MetadataOracleEntity metadataOracleEntity = new MetadataOracleEntity();
        // 如果当前map中没有，说明要重新取值
        if (tablePropertiesMap == null || !tablePropertiesMap.containsKey(tableName)) {
            init();
        }
        OracleTableEntity tableProperties = tablePropertiesMap.get(tableName);
        tableProperties.setCreateTime(DateUtil.stringToDate(createdTimeMap.get(tableName)).getTime());
        List<OracleColumnEntity> columnList = columnListMap.get(tableName);
        List<OracleIndexEntity> indexList = indexListMap.get(tableName);
        String primaryColumn = primaryKeyMap.get(tableName);
        String partitionKey = partitionMap.get(tableName);
        List<OracleColumnEntity> partitionColumnList = new ArrayList<>();
        for (OracleColumnEntity oracleColumnEntity : columnList) {
            if (StringUtils.equals(oracleColumnEntity.getName(), primaryColumn)) {
                oracleColumnEntity.setPrimaryKey(KEY_TRUE);
            } else {
                oracleColumnEntity.setPrimaryKey(KEY_FALSE);
            }
            if (StringUtils.equals(oracleColumnEntity.getName(), partitionKey)) {
                partitionColumnList.add(oracleColumnEntity);
            }
        }
        metadataOracleEntity.setPartitionColumns(partitionColumnList);
        metadataOracleEntity.setTableProperties(tableProperties);
        metadataOracleEntity.setColumns(columnList);
        metadataOracleEntity.setIndexEntities(indexList);
        return metadataOracleEntity;
    }

    /**
     * 查询表的配置信息
     *
     * @return 表的配置信息
     * @throws SQLException sql执行异常
     */
    protected Map<String, OracleTableEntity> queryTableProperties() throws SQLException {
        Map<String, OracleTableEntity> tablePropertiesMap = new HashMap<>(16);
        sql = String.format(SQL_QUERY_TABLE_PROPERTIES_TOTAL, quote(currentDatabase));
        if (StringUtils.isNotBlank(allTable)) {
            sql += String.format(SQL_QUERY_TABLE_PROPERTIES, allTable);
        }
        log.info("querySQL: {}", sql);
        try (ResultSet rs = statement.executeQuery(sql)) {
            while (rs.next()) {
                OracleTableEntity oracleTableEntity = new OracleTableEntity();
                oracleTableEntity.setTotalSize(rs.getLong(1));
                oracleTableEntity.setComment(rs.getString(2));
                oracleTableEntity.setTableType(rs.getString(3));
                oracleTableEntity.setRows(rs.getLong(4));
                tablePropertiesMap.put(rs.getString(5), oracleTableEntity);
            }
        }
        return tablePropertiesMap;
    }

    /**
     * 获取表以及表索引集合，批量查询
     *
     * @return 表以及表索引集合
     * @throws SQLException sql执行异常
     */
    protected Map<String, List<OracleIndexEntity>> queryIndexList() throws SQLException {
        Map<String, List<OracleIndexEntity>> indexListMap = new HashMap<>(16);
        sql = String.format(SQL_QUERY_INDEX_TOTAL, quote(currentDatabase));
        if (StringUtils.isNotBlank(allTable)) {
            sql += String.format(SQL_QUERY_INDEX, allTable);
        }
        log.info("querySQL: {}", sql);
        try (ResultSet rs = statement.executeQuery(sql)) {
            while (rs.next()) {
                OracleIndexEntity oracleIndexEntity = new OracleIndexEntity();
                oracleIndexEntity.setIndexName(rs.getString(1));
                oracleIndexEntity.setColumnName(rs.getString(2));
                oracleIndexEntity.setIndexType(rs.getString(3));
                String tableName = rs.getString(4);
                if (indexListMap.containsKey(tableName)) {
                    indexListMap.get(tableName).add(oracleIndexEntity);
                } else {
                    List<OracleIndexEntity> indexList = new LinkedList<>();
                    indexList.add(oracleIndexEntity);
                    indexListMap.put(tableName, indexList);
                }
            }
        }
        return indexListMap;
    }

    /**
     * 获取表和表字段集合
     *
     * @return 表和表字段集合
     * @throws SQLException sql执行异常
     */
    protected Map<String, List<OracleColumnEntity>> queryColumnList() throws SQLException {
        Map<String, List<OracleColumnEntity>> columnListMap = new HashMap<>(16);
        sql = String.format(SQL_QUERY_COLUMN_TOTAL, quote(currentDatabase));
        if (StringUtils.isNotBlank(allTable)) {
            sql += String.format(SQL_QUERY_COLUMN, allTable);
        }
        log.info("querySQL: {}", sql);
        try (ResultSet rs = statement.executeQuery(sql)) {
            while (rs.next()) {
                OracleColumnEntity oracleColumnEntity = new OracleColumnEntity();
                // oracle中，resultSet的LONG、LONG ROW类型要放第一个取出来
                oracleColumnEntity.setDefaultValue(rs.getString(5));
                oracleColumnEntity.setName(rs.getString(1));
                String type = rs.getString(2);
                Integer length = rs.getInt(7);
                oracleColumnEntity.setType(type);
                oracleColumnEntity.setLength(length);
                String precision = rs.getString(9);
                String scale = rs.getString(10);
                if (StringUtils.isNotBlank(precision)) {
                    oracleColumnEntity.setPrecision(Integer.parseInt(precision));
                }

                if (StringUtils.isNotBlank(scale)) {
                    oracleColumnEntity.setDigital(Integer.parseInt(scale));
                }

                if (StringUtils.equals(type, KEY_NUMBER)) {
                    oracleColumnEntity.setType(String.format(NUMBER_PRECISION, precision == null ? length : precision, scale == null ? KEY_MAX_NUMBER : scale));
                }

                oracleColumnEntity.setComment(rs.getString(3));
                String tableName = rs.getString(4);
                oracleColumnEntity.setNullAble(rs.getString(6));
                Integer index = rs.getInt(8);
                oracleColumnEntity.setIndex(index);
                if (columnListMap.containsKey(tableName)) {
                    columnListMap.get(tableName).add(oracleColumnEntity);
                } else {
                    List<OracleColumnEntity> columnList = new LinkedList<>();
                    columnList.add(oracleColumnEntity);
                    columnListMap.put(tableName, columnList);
                }
            }
        }
        return columnListMap;
    }

    /**
     * 表名和某个特定属性映射的map
     */
    protected void queryTableKeyMap(String type) throws SQLException {
        switch (type) {
            case KEY_PRIMARY_KEY: {
                sql = String.format(SQL_QUERY_PRIMARY_KEY_TOTAL, quote(currentDatabase));
                if (StringUtils.isNotBlank(allTable)) {
                    sql += String.format(SQL_QUERY_PRIMARY_KEY, allTable);
                }
                primaryKeyMap = getResultByMap(sql);
                break;
            }
            case KEY_CREATE_TIME: {
                sql = String.format(SQL_QUERY_TABLE_CREATE_TIME_TOTAL, quote(currentDatabase));
                if (StringUtils.isNotBlank(allTable)) {
                    sql += String.format(SQL_QUERY_TABLE_CREATE_TIME, allTable);
                }
                createdTimeMap = getResultByMap(sql);
                break;
            }
            case KEY_PARTITION_KEY: {
                sql = String.format(SQL_PARTITION_KEY, quote(currentDatabase));
                if (StringUtils.isNotBlank(allTable)) {
                    sql += String.format(SQL_QUERY_TABLE_PARTITION_KEY, allTable);
                }
                partitionMap = getResultByMap(sql);
                break;
            }
            default:
                break;
        }
    }

    /**
     * 根据sql查询集合集
     */
    private Map<String, String> getResultByMap(String sql) throws SQLException {
        Map<String, String> resultMap = Maps.newHashMap();
        log.info("querySQL: {}", sql);
        try (ResultSet rs = statement.executeQuery(sql)) {
            while (rs.next()) {
                resultMap.put(rs.getString(1), rs.getString(2));
            }
        }
        return resultMap;
    }

    /**
     * 每隔20张表进行一次查询
     *
     * @throws SQLException sql执行异常
     */
    protected void init() throws SQLException {
        // 没有表则退出
        if (tableList.size() == 0) {
            return;
        }
        allTable = null;
        if (currentTablePosition < tableList.size()) {
            // 取出子数组，注意避免越界
            List<Object> splitTableList = tableList.subList(currentTablePosition, Math.min(currentTablePosition + MAX_TABLE_SIZE, tableList.size()));
            StringBuilder stringBuilder = new StringBuilder(2 * splitTableList.size());
            for (int index = 0; index < splitTableList.size(); index++) {
                stringBuilder.append(quote((String) splitTableList.get(index)));
                if (index != splitTableList.size() - 1) {
                    stringBuilder.append(COMMA_SYMBOL);
                }
            }
            allTable = stringBuilder.toString();
            tablePropertiesMap = queryTableProperties();
            columnListMap = queryColumnList();
            indexListMap = queryIndexList();
            queryTableKeyMap(KEY_PRIMARY_KEY);
            queryTableKeyMap(KEY_CREATE_TIME);
            //TODO oracle 支持多级分区
            queryTableKeyMap(KEY_PARTITION_KEY);
            currentTablePosition = currentTablePosition + splitTableList.size();
        }
    }

}
