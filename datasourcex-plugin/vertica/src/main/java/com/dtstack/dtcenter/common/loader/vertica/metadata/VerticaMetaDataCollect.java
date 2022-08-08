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

package com.dtstack.dtcenter.common.loader.vertica.metadata;

import com.dtstack.dtcenter.common.loader.common.utils.DateUtil;
import com.dtstack.dtcenter.common.loader.metadata.constants.ConstantValue;
import com.dtstack.dtcenter.common.loader.metadata.core.MetadataBaseCollectSplit;
import com.dtstack.dtcenter.common.loader.rdbms.ConnFactory;
import com.dtstack.dtcenter.common.loader.rdbms.metadata.RdbmsMetaDataCollect;
import com.dtstack.dtcenter.common.loader.vertica.VerticaConnFactory;
import com.dtstack.dtcenter.common.loader.vertica.metadata.constants.VerticaMetaDataCons;
import com.dtstack.dtcenter.loader.dto.metadata.MetadataCollectCondition;
import com.dtstack.dtcenter.loader.dto.metadata.entity.MetadataEntity;
import com.dtstack.dtcenter.loader.dto.metadata.entity.rdb.ColumnEntity;
import com.dtstack.dtcenter.loader.dto.metadata.entity.rdb.MetadataRdbEntity;
import com.dtstack.dtcenter.loader.dto.metadata.entity.rdb.TableEntity;
import com.dtstack.dtcenter.loader.dto.metadata.entity.vertica.MetadataVerticaEntity;
import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

@Slf4j
public class VerticaMetaDataCollect extends RdbmsMetaDataCollect {

    /**创建时间集合*/
    protected Map<String, String> createTimeMap;

    /**表的描述集合*/
    protected Map<String, String> commentMap;

    /**表大小集合*/
    protected Map<String, String> totalSizeMap;

    /**分区字段集合*/
    protected Map<String, String> ptColumnMap;

    List<ColumnEntity> ptColumns = new LinkedList<>();

    /**
     * 采用数组是为了构建Varchar(10)、Decimal(10,2)这种格式
     */
    private static final List<String> SINGLE_DIGITAL_TYPE = Arrays.asList("Integer", "Varchar", "Char", "Numeric");

    private static final List<String> DOUBLE_DIGITAL_TYPE = Arrays.asList("Timestamp", "Decimal");

    @Override
    public void init(ISourceDTO sourceDTO, MetadataCollectCondition metadataCollectCondition,
                     MetadataBaseCollectSplit metadataBaseCollectSplit, Queue<MetadataEntity> metadataEntities) throws Exception {
        log.info("init collect Split start: {} ", metadataBaseCollectSplit);
        this.sourceDTO = sourceDTO;
        this.metadataCollectCondition = metadataCollectCondition;
        this.metadataBaseCollectSplit = metadataBaseCollectSplit;
        this.tableList = metadataBaseCollectSplit.getTableList();
        this.currentDatabase = metadataBaseCollectSplit.getDbName();
        super.metadataEntities = metadataEntities;
        // 建立连接
        openConnection();
        this.iterator = tableList.iterator();
        log.info("init collect Split end : {}", metadataBaseCollectSplit);
        IS_INIT.set(true);
        queryCreateTime();
        queryComment();
        queryTotalSizeMap();
        queryPtColumnMap();
    }

    @Override
    protected MetadataRdbEntity createMetadataRdbEntity() {
        return queryMetaData((String) currentObject);
    }

    protected MetadataVerticaEntity queryMetaData(String tableName) {
        MetadataVerticaEntity metadataVerticaEntity = new MetadataVerticaEntity();
        metadataVerticaEntity.setPartitionColumns(ptColumns);
        ptColumns.clear();
        metadataVerticaEntity.setTableProperties(queryTableProp(tableName));
        metadataVerticaEntity.setColumns(queryColumn(null));
        return metadataVerticaEntity;
    }

    /**
     * 获取表级别的元数据信息
     *
     * @param tableName 表名
     * @return 表的元数据
     */
    public TableEntity queryTableProp(String tableName) {
        TableEntity verticaTableEntity = new TableEntity();
        verticaTableEntity.setCreateTime(DateUtil.stringToDate(createTimeMap.get(tableName)).getTime());
        verticaTableEntity.setComment(commentMap.get(tableName));
        // 单位 byte
        String totalSize = totalSizeMap.get(tableName);
        totalSize = totalSize == null ? "0" : totalSize;
        verticaTableEntity.setTotalSize(Long.valueOf(totalSize));
        return verticaTableEntity;
    }

    @Override
    public List<Object> showTables() {
        List<Object> tables = new LinkedList<>();
        try (ResultSet resultSet = connection.getMetaData().getTables(null, currentDatabase, null, null)) {
            while (resultSet.next()) {
                tables.add(resultSet.getString(VerticaMetaDataCons.RESULT_SET_TABLE_NAME));
            }
        } catch (SQLException e) {
            throw new DtLoaderException("sql exception");
        }
        return tables;
    }

    @Override
    public List<ColumnEntity> queryColumn(String schema) {
        List<ColumnEntity> columns = new LinkedList<>();
        String tableName = (String) currentObject;
        try (ResultSet resultSet = connection.getMetaData().getColumns(null, currentDatabase, tableName, null)) {
            while (resultSet.next()) {
                ColumnEntity verticaColumnEntity = new ColumnEntity();
                String columnName = resultSet.getString(VerticaMetaDataCons.RESULT_SET_COLUMN_NAME);
                verticaColumnEntity.setName(columnName);
                String dataSize = resultSet.getString(VerticaMetaDataCons.RESULT_SET_COLUMN_SIZE);
                String digits = resultSet.getString(VerticaMetaDataCons.RESULT_SET_DECIMAL_DIGITS);
                String type = resultSet.getString(VerticaMetaDataCons.RESULT_SET_TYPE_NAME);
                if (SINGLE_DIGITAL_TYPE.contains(type)) {
                    type += ConstantValue.LEFT_PARENTHESIS_SYMBOL + dataSize + ConstantValue.RIGHT_PARENTHESIS_SYMBOL;
                } else if (DOUBLE_DIGITAL_TYPE.contains(type)) {
                    type += ConstantValue.LEFT_PARENTHESIS_SYMBOL + dataSize + ConstantValue.COMMA_SYMBOL + digits + ConstantValue.RIGHT_PARENTHESIS_SYMBOL;
                }
                verticaColumnEntity.setType(type);
                verticaColumnEntity.setComment(resultSet.getString(VerticaMetaDataCons.RESULT_SET_REMARKS));
                verticaColumnEntity.setIndex(resultSet.getInt(VerticaMetaDataCons.RESULT_SET_ORDINAL_POSITION));
                verticaColumnEntity.setNullAble(resultSet.getString(VerticaMetaDataCons.RESULT_SET_IS_NULLABLE));
                verticaColumnEntity.setDefaultValue(resultSet.getString(VerticaMetaDataCons.RESULT_SET_COLUMN_DEF));
                // 分区列信息,vertical partition express 中字段自动增加表名
                String expressColumn = tableName + ConstantValue.POINT_SYMBOL + columnName;
                String partitionExpression = ptColumnMap.get(tableName);
                if (StringUtils.isNotBlank(partitionExpression) && partitionExpression.contains(expressColumn)) {
                    ptColumns.add(verticaColumnEntity);
                } else {
                    columns.add(verticaColumnEntity);
                }
            }
        } catch (SQLException e) {
            throw new DtLoaderException("query columns failed");
        }
        return columns;
    }

    /**
     * 获取创建时间
     */
    public void queryCreateTime() {
        createTimeMap = new HashMap<>(16);
        String sql = String.format(VerticaMetaDataCons.SQL_CREATE_TIME, currentDatabase);
        try (ResultSet resultSet = executeQuery0(sql, statement)) {
            while (resultSet.next()) {
                createTimeMap.put(resultSet.getString(1), resultSet.getString(2));
            }
        } catch (SQLException e) {
            throw new DtLoaderException("query create time failed");
        }
    }

    /**
     * 获取表注释
     */
    public void queryComment() {
        commentMap = new HashMap<>(16);
        String sql = String.format(VerticaMetaDataCons.SQL_COMMENT, currentDatabase);
        try (ResultSet resultSet = executeQuery0(sql, statement)) {
            while (resultSet.next()) {
                commentMap.put(resultSet.getString(1), resultSet.getString(2));
            }
        } catch (SQLException e) {
            throw new DtLoaderException("query comment failed");
        }
    }

    /**
     * 获取表的总大小
     */
    public void queryTotalSizeMap() {
        totalSizeMap = new HashMap<>(16);
        String sql = String.format(VerticaMetaDataCons.SQL_TOTAL_SIZE, currentDatabase);
        try (ResultSet resultSet = executeQuery0(sql, statement)) {
            while (resultSet.next()) {
                totalSizeMap.put(resultSet.getString(1), resultSet.getString(2));
            }
        } catch (SQLException e) {
            throw new DtLoaderException("query totalSize failed");
        }
    }

    /**
     * 获取分区列信息
     */
    public void queryPtColumnMap() {
        ptColumnMap = new HashMap<>(16);
        String sql = String.format(VerticaMetaDataCons.SQL_PT_COLUMN, currentDatabase);
        try (ResultSet resultSet = executeQuery0(sql, statement)) {
            while (resultSet.next()) {
                String expression = resultSet.getString(2);
                ptColumnMap.put(resultSet.getString(1), expression);
            }
        } catch (SQLException e) {
            throw new DtLoaderException("query partition columns failed");
        }
    }

    @Override
    protected ConnFactory getConnFactory() {
        return new VerticaConnFactory();
    }
}
