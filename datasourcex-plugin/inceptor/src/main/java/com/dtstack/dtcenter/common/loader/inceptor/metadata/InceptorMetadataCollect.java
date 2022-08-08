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

package com.dtstack.dtcenter.common.loader.inceptor.metadata;

import com.dtstack.dtcenter.common.loader.common.utils.DBUtil;
import com.dtstack.dtcenter.common.loader.inceptor.InceptorConnFactory;
import com.dtstack.dtcenter.common.loader.inceptor.InceptorDriverUtil;
import com.dtstack.dtcenter.common.loader.inceptor.metadata.entity.InceptorAnalyzeEntity;
import com.dtstack.dtcenter.common.loader.inceptor.metadata.entity.InceptorTableEntity;
import com.dtstack.dtcenter.common.loader.inceptor.metadata.entity.MetadataInceptorEntity;
import com.dtstack.dtcenter.common.loader.rdbms.ConnFactory;
import com.dtstack.dtcenter.common.loader.rdbms.metadata.RdbmsMetaDataCollect;
import com.dtstack.dtcenter.loader.dto.metadata.entity.rdb.ColumnEntity;
import com.dtstack.dtcenter.loader.dto.metadata.entity.rdb.MetadataRdbEntity;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static com.dtstack.dtcenter.common.loader.inceptor.metadata.cons.InceptorCons.COLUMN_QUERY_DATA;
import static com.dtstack.dtcenter.common.loader.inceptor.metadata.cons.InceptorCons.KEY_ATTRIBUTE;
import static com.dtstack.dtcenter.common.loader.inceptor.metadata.cons.InceptorCons.KEY_CATEGORY;
import static com.dtstack.dtcenter.common.loader.inceptor.metadata.cons.InceptorCons.KEY_COL_CREATETIME;
import static com.dtstack.dtcenter.common.loader.inceptor.metadata.cons.InceptorCons.KEY_COL_CREATE_TIME;
import static com.dtstack.dtcenter.common.loader.inceptor.metadata.cons.InceptorCons.KEY_COL_LASTACCESSTIME;
import static com.dtstack.dtcenter.common.loader.inceptor.metadata.cons.InceptorCons.KEY_COL_LAST_ACCESS_TIME;
import static com.dtstack.dtcenter.common.loader.inceptor.metadata.cons.InceptorCons.KEY_COL_LOCATION;
import static com.dtstack.dtcenter.common.loader.inceptor.metadata.cons.InceptorCons.KEY_COL_NAME;
import static com.dtstack.dtcenter.common.loader.inceptor.metadata.cons.InceptorCons.KEY_COL_SERDE_LIBRARY;
import static com.dtstack.dtcenter.common.loader.inceptor.metadata.cons.InceptorCons.KEY_COMMENT;
import static com.dtstack.dtcenter.common.loader.inceptor.metadata.cons.InceptorCons.NUM_BUCKETS;
import static com.dtstack.dtcenter.common.loader.inceptor.metadata.cons.InceptorCons.PARTITION_INFORMATION;
import static com.dtstack.dtcenter.common.loader.inceptor.metadata.cons.InceptorCons.SQL_ANALYZE_DATA;
import static com.dtstack.dtcenter.common.loader.inceptor.metadata.cons.InceptorCons.SQL_QUERY_DATA;
import static com.dtstack.dtcenter.common.loader.inceptor.metadata.cons.InceptorCons.SQL_SHOW_TABLES;
import static com.dtstack.dtcenter.common.loader.inceptor.metadata.cons.InceptorCons.SQL_SWITCH_DATABASE;
import static com.dtstack.dtcenter.common.loader.inceptor.metadata.cons.InceptorCons.STORAGE_INFORMATION;
import static com.dtstack.dtcenter.common.loader.inceptor.metadata.cons.InceptorCons.TABLE_INFORMATION;
import static com.dtstack.dtcenter.common.loader.inceptor.metadata.cons.InceptorCons.TABLE_ROWS;
import static com.dtstack.dtcenter.common.loader.inceptor.metadata.cons.InceptorCons.TABLE_SIZE;
import static com.dtstack.dtcenter.common.loader.inceptor.metadata.cons.InceptorCons.TABLE_TYPE;
import static com.dtstack.dtcenter.common.loader.inceptor.metadata.cons.InceptorCons.TRANSIENT_LASTDDLTIME;

/**
 * inceptor元数据采集器
 *
 * @author luming
 * @date 2022/4/15
 */
@Slf4j
public class InceptorMetadataCollect extends RdbmsMetaDataCollect {

    @Override
    public List<Object> showTables() {
        List<Object> tables = new ArrayList<>();
        try (ResultSet rs = statement.executeQuery(SQL_SHOW_TABLES)) {
            int pos = rs.getMetaData().getColumnCount() == 1 ? 1 : 2;
            while (rs.next()) {
                tables.add(rs.getString(pos));
            }
        } catch (Exception e) {
            throw new DtLoaderException("query inceptor table error.", e);
        }

        return tables;
    }

    @Override
    public void switchDataBase() throws SQLException {
        statement.execute(String.format(SQL_SWITCH_DATABASE, quote(currentDatabase)));
    }

    @Override
    protected MetadataRdbEntity createMetadataRdbEntity() {
        MetadataInceptorEntity entity = new MetadataInceptorEntity();
        List<ColumnEntity> columnList = new ArrayList<>();
        List<ColumnEntity> partitionColumnList = new ArrayList<>();
        InceptorTableEntity tableProperties = new InceptorTableEntity();
        String tableName = (String) currentObject;
        List<Map<String, String>> metaData = queryData(tableName);
        // 查询数据的行数和总的文件大小
        List<Map<String, String>> analysisInfo = queryAnalyzeData(tableName);
        metaData.addAll(analysisInfo);
        Iterator<Map<String, String>> it = metaData.iterator();
        int metaDataFlag = 0;
        boolean isPartition = false;
        while (it.hasNext()) {
            Map<String, String> lineDataInternal = it.next();
            String colNameInternal = lineDataInternal.get(KEY_CATEGORY);
            if (StringUtils.isBlank(colNameInternal)) {
                continue;
            }
            if (colNameInternal.startsWith("#")) {
                colNameInternal = StringUtils.trim(colNameInternal);
                switch (colNameInternal) {
                    case PARTITION_INFORMATION:
                        metaDataFlag = 1;
                        isPartition = true;
                        break;
                    case TABLE_INFORMATION:
                    case STORAGE_INFORMATION:
                        metaDataFlag = 2;
                        break;
                    default:
                        break;
                }
                continue;
            }
            switch (metaDataFlag) {
                case 0:
                    // inceptor 数据列
                    columnList.add(parseColumn(lineDataInternal, columnList.size() + 1));
                    break;
                case 1:
                    // inceptor 分区列
                    partitionColumnList.add(
                            parseColumn(lineDataInternal, partitionColumnList.size() + 1));
                    break;
                case 2:
                    // # Detailed Table Information 信息
                    parseTableProperties(lineDataInternal, tableProperties);
                    break;
                default:
                    break;
            }
        }
        if (partitionColumnList.size() > 0) {
            List<String> partitionColumnNames = new ArrayList<>();
            for (ColumnEntity partitionColumn : partitionColumnList) {
                partitionColumnNames.add(partitionColumn.getName());
            }
            columnList.removeIf(column -> partitionColumnNames.contains(column.getName()));
            entity.setPartitions(partitionColumnNames);
        }
        if (columnList.size() > 0) columnList = queryColumnDetail(tableName, columnList);
        // 如果存在 # Partition Information 存在分区
        tableProperties.setIsPartition(isPartition ? "true" : "false");
        entity.setTableProperties(tableProperties);
        entity.setPartitionColumns(partitionColumnList);
        entity.setColumns(columnList);
        return entity;
    }

    @Override
    protected ConnFactory getConnFactory() {
        return new InceptorConnFactory();
    }

    private String quote(String name) {
        return String.format("`%s`", name);
    }

    private List<ColumnEntity> queryColumnDetail(String table, List<ColumnEntity> columnList) {
        Map<String, ColumnEntity> columnMap = new HashMap<>();
        columnList.forEach(c -> columnMap.put(c.getName(), c));

        for (int time = 0; true; time++) {
            try (ResultSet rs =
                         statement.executeQuery(String.format(COLUMN_QUERY_DATA, quote(table)))) {
                while (rs.next()) {
                    ColumnEntity columnEntity = columnMap.get(rs.getString(KEY_COL_NAME));
                    if (columnEntity != null) columnEntity.setComment(rs.getString(KEY_COMMENT));
                }
                return new ArrayList<>(columnMap.values());
            } catch (Exception e) {
                log.warn("times : {}, query data error", time, e);
                //第三次还有异常就不重试了
                if (time == 2) {
                    throw new DtLoaderException("hive query data three times, still error.", e);
                }
                resetConnection();
            }
        }
    }

    /**
     * 重新获取connection
     */
    private void resetConnection() {
        try {
            DBUtil.closeDBResources(null, statement, connection);
            connection = connFactory.getConn(sourceDTO);
            statement = connection.createStatement();
            switchDataBase();
        } catch (Exception e) {
            log.warn("connection is not valid and an exception occurred while closing", e);
            throw new DtLoaderException("", e);
        }
    }

    /**
     * 解析表的参数
     *
     * @param lineDataInternal 原始表信息
     * @param tableProperties  待填充对象
     */
    private void parseTableProperties(Map<String, String> lineDataInternal,
                                      InceptorTableEntity tableProperties) {
        String attributeNmae = StringUtils.trim(lineDataInternal.get(KEY_CATEGORY));
        String attributeValue = StringUtils.trim(lineDataInternal.get(KEY_ATTRIBUTE));
        switch (attributeNmae) {
            case KEY_COL_LOCATION:
                tableProperties.setLocation(attributeValue);
                break;
            case KEY_COL_CREATETIME:
            case KEY_COL_CREATE_TIME:
//                tableProperties.setCreateTime(attributeValue);
                break;
            case KEY_COL_LASTACCESSTIME:
            case KEY_COL_LAST_ACCESS_TIME:
                tableProperties.setLastAccessTime(attributeValue);
                break;
            case KEY_COL_SERDE_LIBRARY:
                tableProperties.setStoreType(InceptorDriverUtil.getStoredType(attributeValue));
                break;
            case TABLE_ROWS:
                tableProperties.setTableRows(attributeValue);
                break;
            case TABLE_SIZE:
                tableProperties.setTableSize(attributeValue);
                break;
            case NUM_BUCKETS:
                tableProperties.setIsBucked(
                        Integer.parseInt(attributeValue) != -1 ? "true" : "false");
                break;
            case TABLE_TYPE:
                tableProperties.setTableType(attributeValue);
                break;
            case TRANSIENT_LASTDDLTIME:
                tableProperties.setTransientLastDdlTime(attributeValue);
                break;
            case KEY_COMMENT:
                tableProperties.setComment(attributeValue);
            default:
                break;
        }
    }

    /**
     * 查询表元数据
     *
     * @param table 表名
     * @return 元数据
     */
    private List<Map<String, String>> queryData(String table) {
        for (int time = 0; true; time++) {
            try (ResultSet rs =
                         statement.executeQuery(String.format(SQL_QUERY_DATA, quote(table)))) {
                ResultSetMetaData metaData = rs.getMetaData();
                int columnCount = metaData.getColumnCount();
                List<String> columnNames = new ArrayList<>(columnCount);
                for (int i = 0; i < columnCount; i++) {
                    columnNames.add(metaData.getColumnName(i + 1));
                }
                List<Map<String, String>> data = new ArrayList<>();
                while (rs.next()) {
                    Map<String, String> lineData =
                            new HashMap<>(Math.max((int) (columnCount / .75f) + 1, 16));
                    for (String columnName : columnNames) {
                        lineData.put(columnName, rs.getString(columnName));
                    }
                    data.add(lineData);
                }
                return data;
            } catch (Exception e) {
                log.warn("times : {}, query data error", time, e);
                //第三次还有异常就不重试了
                if (time == 2) {
                    throw new DtLoaderException("hive query data three times, still error.", e);
                }
                resetConnection();
            }
        }
    }

    /**
     * 查询表元数据
     *
     * @param table 表名
     * @return 元数据
     */
    private List<Map<String, String>> queryAnalyzeData(String table) {
        for (int time = 0; true; time++) {
            try (ResultSet rs =
                         statement.executeQuery(String.format(SQL_ANALYZE_DATA, quote(table)))) {
                List<Map<String, String>> data = new ArrayList<>();
                LinkedList<InceptorAnalyzeEntity> inceptorAnalyzeEntityList = new LinkedList<>();
                while (rs.next()) {
                    inceptorAnalyzeEntityList.add(resolveStatistics(rs.getString(2)));
                }
                // 累加计算所有分区的行数和分区大小，就是表的分区行数和表的分区大小。
                Long tableRows = 0L;
                Long tableSize = 0L;
                for (InceptorAnalyzeEntity inceptorAnalyzeEntity : inceptorAnalyzeEntityList) {
                    tableRows += inceptorAnalyzeEntity.getNumRows();
                    tableSize += inceptorAnalyzeEntity.getTotalSize();
                }
                Map<String, String> tableNumRowsKV = new HashMap<>(2);
                Map<String, String> tableTotalSizeKV = new HashMap<>(2);
                tableNumRowsKV.put(KEY_CATEGORY, TABLE_ROWS);
                tableNumRowsKV.put(KEY_ATTRIBUTE, tableRows.toString());
                tableTotalSizeKV.put(KEY_CATEGORY, TABLE_SIZE);
                tableTotalSizeKV.put(KEY_ATTRIBUTE, tableSize.toString());
                data.add(tableNumRowsKV);
                data.add(tableTotalSizeKV);
                return data;
            } catch (Exception e) {
                log.warn("times : {}, query data error", time, e);
                if (time == 2) {
                    throw new DtLoaderException("hive query data three times, still error.", e);
                }
                resetConnection();
            }
        }
    }

    private InceptorAnalyzeEntity resolveStatistics(String statistics) {
        String[] kvStrings = statistics.substring(1, statistics.length() - 1).split(",");
        InceptorAnalyzeEntity inceptorAnalyzeEntity = new InceptorAnalyzeEntity();
        inceptorAnalyzeEntity.setNumFiles(Integer.parseInt(kvStrings[0].split("=")[1]));
        inceptorAnalyzeEntity.setNumRows(Long.parseLong(kvStrings[1].split("=")[1]));
        inceptorAnalyzeEntity.setTotalSize(Long.parseLong(kvStrings[2].split("=")[1]));
        inceptorAnalyzeEntity.setRawDataSize(Long.parseLong(kvStrings[3].split("=")[1]));
        return inceptorAnalyzeEntity;
    }

    /**
     * 解析字段信息
     *
     * @param lineDataInternal 原始字段信息
     * @param index            索引
     * @return 字段entity
     */
    private ColumnEntity parseColumn(Map<String, String> lineDataInternal, int index) {
        ColumnEntity InceptorColumnEntity = new ColumnEntity();
        String columnName = lineDataInternal.get(KEY_CATEGORY);
        String columntype = lineDataInternal.get(KEY_ATTRIBUTE);
        InceptorColumnEntity.setType(columntype);
        // 处理 decimal(10,2) 类型
        if (columntype.contains("(") && columntype.contains(")")) {
            int precisionEndPosition =
                    !columntype.contains(",") ? columntype.indexOf(")") : columntype.indexOf(",");
            String precision =
                    StringUtils.substring(
                            columntype, columntype.indexOf("(") + 1, precisionEndPosition);
            InceptorColumnEntity.setPrecision(Integer.parseInt(precision));
            if (precisionEndPosition < columntype.length() - 1) {
                InceptorColumnEntity.setDigital(
                        Integer.parseInt(
                                StringUtils.substring(
                                        columntype,
                                        precisionEndPosition + 1,
                                        columntype.length() - 1)));
            }
        }
        InceptorColumnEntity.setName(columnName);
        InceptorColumnEntity.setIndex(index);
        return InceptorColumnEntity;
    }
}
