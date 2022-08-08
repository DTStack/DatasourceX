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

package com.dtstack.dtcenter.common.loader.hive2.metadata;

import com.dtstack.dtcenter.common.loader.common.utils.DBUtil;
import com.dtstack.dtcenter.common.loader.common.utils.DateUtil;
import com.dtstack.dtcenter.common.loader.hive2.HiveConnFactory;
import com.dtstack.dtcenter.common.loader.hive2.HiveDriverUtil;
import com.dtstack.dtcenter.common.loader.hive2.metadata.entity.HiveTableEntity;
import com.dtstack.dtcenter.common.loader.hive2.metadata.entity.MetadataHiveEntity;
import com.dtstack.dtcenter.common.loader.rdbms.ConnFactory;
import com.dtstack.dtcenter.common.loader.rdbms.metadata.RdbmsMetaDataCollect;
import com.dtstack.dtcenter.loader.dto.metadata.entity.rdb.ColumnEntity;
import com.dtstack.dtcenter.loader.dto.metadata.entity.rdb.MetadataRdbEntity;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.dtstack.dtcenter.common.loader.hive2.metadata.cons.HiveCons.ANALYZE_TABLE;
import static com.dtstack.dtcenter.common.loader.hive2.metadata.cons.HiveCons.KEY_COLUMN_DATA_TYPE;
import static com.dtstack.dtcenter.common.loader.hive2.metadata.cons.HiveCons.KEY_COL_CREATETIME;
import static com.dtstack.dtcenter.common.loader.hive2.metadata.cons.HiveCons.KEY_COL_CREATE_TIME;
import static com.dtstack.dtcenter.common.loader.hive2.metadata.cons.HiveCons.KEY_COL_LASTACCESSTIME;
import static com.dtstack.dtcenter.common.loader.hive2.metadata.cons.HiveCons.KEY_COL_LAST_ACCESS_TIME;
import static com.dtstack.dtcenter.common.loader.hive2.metadata.cons.HiveCons.KEY_COL_LOCATION;
import static com.dtstack.dtcenter.common.loader.hive2.metadata.cons.HiveCons.KEY_COL_NAME;
import static com.dtstack.dtcenter.common.loader.hive2.metadata.cons.HiveCons.KEY_COL_OUTPUTFORMAT;
import static com.dtstack.dtcenter.common.loader.hive2.metadata.cons.HiveCons.KEY_COL_TABLE_PARAMETERS;
import static com.dtstack.dtcenter.common.loader.hive2.metadata.cons.HiveCons.KEY_COMMENT;
import static com.dtstack.dtcenter.common.loader.hive2.metadata.cons.HiveCons.KEY_NUMROWS;
import static com.dtstack.dtcenter.common.loader.hive2.metadata.cons.HiveCons.KEY_SPARK_NUMROWS;
import static com.dtstack.dtcenter.common.loader.hive2.metadata.cons.HiveCons.KEY_SPARK_TOTALSIZE;
import static com.dtstack.dtcenter.common.loader.hive2.metadata.cons.HiveCons.KEY_TOTALSIZE;
import static com.dtstack.dtcenter.common.loader.hive2.metadata.cons.HiveCons.KEY_TRANSIENT_LASTDDLTIME;
import static com.dtstack.dtcenter.common.loader.hive2.metadata.cons.HiveCons.PARTITION_INFORMATION;
import static com.dtstack.dtcenter.common.loader.hive2.metadata.cons.HiveCons.SQL_QUERY_DATA;
import static com.dtstack.dtcenter.common.loader.hive2.metadata.cons.HiveCons.SQL_SHOW_TABLES;
import static com.dtstack.dtcenter.common.loader.hive2.metadata.cons.HiveCons.SQL_SWITCH_DATABASE;
import static com.dtstack.dtcenter.common.loader.hive2.metadata.cons.HiveCons.TABLE_INFORMATION;

/**
 * hive2.x元数据收集器
 *
 * @author luming
 * @date 2022/4/14
 */
@Slf4j
public class Hive2MetadataCollect extends RdbmsMetaDataCollect {

    @Override
    public void switchDataBase() throws SQLException {
        statement.execute(String.format(SQL_SWITCH_DATABASE, quote(currentDatabase)));
    }

    @Override
    protected List<Object> showTables() {
        List<Object> tables = new ArrayList<>();
        try (ResultSet rs = statement.executeQuery(SQL_SHOW_TABLES)) {
            int pos = rs.getMetaData().getColumnCount() == 1 ? 1 : 2;
            while (rs.next()) {
                tables.add(rs.getString(pos));
            }
        } catch (Exception e) {
            throw new DtLoaderException("hive query tables error.", e);
        }

        return tables;
    }

    @Override
    protected MetadataRdbEntity createMetadataRdbEntity() {
        MetadataHiveEntity hiveEntity = new MetadataHiveEntity();
        List<ColumnEntity> columnList = new ArrayList<>();
        List<ColumnEntity> partitionColumnList = new ArrayList<>();
        HiveTableEntity tableProperties = new HiveTableEntity();
        String tableName = (String) currentObject;

        preParseTable(tableName);

        List<Map<String, String>> metaData = queryData(tableName);
        Iterator<Map<String, String>> it = metaData.iterator();
        int metaDataFlag = 0;
        while (it.hasNext()) {
            Map<String, String> lineDataInternal = it.next();
            String colNameInternal = lineDataInternal.get(KEY_COL_NAME);
            if (StringUtils.isBlank(colNameInternal)) {
                continue;
            }
            if (colNameInternal.startsWith("#")) {
                colNameInternal = StringUtils.trim(colNameInternal);
                switch (colNameInternal) {
                    case PARTITION_INFORMATION:
                        metaDataFlag = 1;
                        break;
                    case TABLE_INFORMATION:
                        metaDataFlag = 2;
                        break;
                    default:
                        break;
                }
                continue;
            }
            switch (metaDataFlag) {
                case 0:
                    columnList.add(parseColumn(lineDataInternal, columnList.size() + 1));
                    break;
                case 1:
                    partitionColumnList.add(parseColumn(lineDataInternal, partitionColumnList.size() + 1));
                    break;
                case 2:
                    parseTableProperties(lineDataInternal, tableProperties, it);
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
            hiveEntity.setPartitions(partitionColumnNames);
        }
        hiveEntity.setTableProperties(tableProperties);
        hiveEntity.setPartitionColumns(partitionColumnList);
        hiveEntity.setColumns(columnList);
        hiveEntity.setQuerySuccess(true);
        return hiveEntity;
    }

    @Override
    protected ConnFactory getConnFactory() {
        return new HiveConnFactory();
    }

    private String quote(String name) {
        return String.format("`%s`", name);
    }

    /**
     * 在解析表信息之前执行的操作
     */
    private void preParseTable(String tableName) {
        List<String> partitionColumnList = new ArrayList<>();
        try {
            List<Map<String, String>> metaData = queryData(tableName);
            Iterator<Map<String, String>> it = metaData.iterator();
            int metaDataFlag = 0;
            while (it.hasNext()) {
                Map<String, String> lineDataInternal = it.next();
                String colNameInternal = lineDataInternal.get(KEY_COL_NAME);
                if (StringUtils.isBlank(colNameInternal)) {
                    continue;
                }
                if (colNameInternal.startsWith("#")) {
                    colNameInternal = StringUtils.trim(colNameInternal);
                    switch (colNameInternal) {
                        case PARTITION_INFORMATION:
                            metaDataFlag = 1;
                            break;
                        case TABLE_INFORMATION:
                            metaDataFlag = 2;
                            break;
                        default:
                            break;
                    }
                    continue;
                }
                if (metaDataFlag == 1) {
                    partitionColumnList.add(lineDataInternal.get(KEY_COL_NAME));
                }
            }
            analyzeTable(tableName, partitionColumnList);
        } catch (Exception e) {
            throw new DtLoaderException("get partiitonCol failed ", e);
        }
    }

    /**
     * 查询表元数据
     *
     * @param table 表名
     * @return 表信息
     */
    private List<Map<String, String>> queryData(String table) {
        //三次重试
        for (int time = 0; true; time++) {
            try (ResultSet rs = statement.executeQuery(String.format(SQL_QUERY_DATA, quote(table)))) {
                ResultSetMetaData metaData = rs.getMetaData();
                int columnCount = metaData.getColumnCount();
                List<String> columnNames = new ArrayList<>(columnCount);
                for (int i = 0; i < columnCount; i++) {
                    columnNames.add(metaData.getColumnName(i + 1));
                }
                List<Map<String, String>> data = new ArrayList<>();
                while (rs.next()) {
                    Map<String, String> lineData = new HashMap<>(Math.max((int) (columnCount / .75f) + 1, 16));
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
     * 解析字段信息
     *
     * @param lineDataInternal 行字段信息map
     * @param index            字段下标
     * @return columnEntity
     */
    private ColumnEntity parseColumn(Map<String, String> lineDataInternal, int index) {
        ColumnEntity hiveColumnEntity = new ColumnEntity();
        String dataTypeInternal = lineDataInternal.get(KEY_COLUMN_DATA_TYPE);
        String commentInternal = lineDataInternal.get(KEY_COMMENT);
        String colNameInternal = lineDataInternal.get(KEY_COL_NAME);
        hiveColumnEntity.setType(dataTypeInternal);
        if (dataTypeInternal.contains("(") && dataTypeInternal.contains(")")) {
            int precisionEndPosition = !dataTypeInternal.contains(",")
                    ? dataTypeInternal.indexOf(")") : dataTypeInternal.indexOf(",");
            String precision = StringUtils
                    .substring(dataTypeInternal, dataTypeInternal.indexOf("(") + 1, precisionEndPosition);
            hiveColumnEntity.setPrecision(Integer.parseInt(precision));
            if (precisionEndPosition < dataTypeInternal.length() - 1) {
                hiveColumnEntity.setDigital(
                        Integer.parseInt(StringUtils.substring(
                                dataTypeInternal, precisionEndPosition + 1, dataTypeInternal.length() - 1)));
            }
        }
        hiveColumnEntity.setComment(commentInternal);
        hiveColumnEntity.setName(colNameInternal);
        hiveColumnEntity.setIndex(index);
        return hiveColumnEntity;
    }

    /**
     * 执行 analyzeTable 刷新元数据信息
     */
    private void analyzeTable(String tableName, List<String> partitionColumnList) {
        try {
            String partition = "";
            if (CollectionUtils.isNotEmpty(partitionColumnList)) {
                partition = "partition(" + String.join(",", partitionColumnList) + ")";
            }

            String sql = String.format(ANALYZE_TABLE, tableName, partition);
            Statement statement = connection.createStatement();
            statement.execute(sql);
        } catch (Exception e) {
            log.warn("execute analyzeTable for " + tableName + " failed", e);
        }
    }

    /**
     * 解析表的参数
     *
     * @param lineDataInternal
     * @param tableProperties
     * @param it
     */
    private void parseTableProperties(Map<String, String> lineDataInternal,
                              HiveTableEntity tableProperties,
                              Iterator<Map<String, String>> it) {
        String name = lineDataInternal.get(KEY_COL_NAME);

        if (name.contains(KEY_COL_LOCATION)) {
            tableProperties.setLocation(StringUtils.trim(lineDataInternal.get(KEY_COLUMN_DATA_TYPE)));
        }
        if (name.contains(KEY_COL_CREATETIME) || name.contains(KEY_COL_CREATE_TIME)) {
            tableProperties.setCreateTime(DateUtil.stringToLong(StringUtils.trim(lineDataInternal.get(KEY_COLUMN_DATA_TYPE))));
        }
        if (name.contains(KEY_COL_LASTACCESSTIME) || name.contains(KEY_COL_LAST_ACCESS_TIME)) {
            String laTime = StringUtils.trim(lineDataInternal.get(KEY_COLUMN_DATA_TYPE));
            if (!laTime.toLowerCase().contains("unknown")){
                tableProperties.setLastAccessTime(laTime);
            }
        }
        if (name.contains(KEY_COL_OUTPUTFORMAT)) {
            String storedClass = lineDataInternal.get(KEY_COLUMN_DATA_TYPE);
            tableProperties.setStoreType(HiveDriverUtil.getStoredType(storedClass));
        }
        if (name.contains(KEY_COL_TABLE_PARAMETERS)) {
            int find = 0;
            boolean totalSize = false;
            boolean numRows = false;
            while (it.hasNext()) {
                lineDataInternal = it.next();
                String nameInternal = lineDataInternal.get(KEY_COL_NAME);
                String dataTypeInternal = lineDataInternal.get(KEY_COLUMN_DATA_TYPE);
                String valueKey = KEY_COLUMN_DATA_TYPE;
                if (StringUtils.isBlank(nameInternal)) {
                    if (StringUtils.isBlank(dataTypeInternal)) {
                        continue;
                    } else {
                        nameInternal = dataTypeInternal;
                        valueKey = KEY_COMMENT;
                    }
                }

                nameInternal = nameInternal.trim();
                if (nameInternal.contains(KEY_COMMENT)) {
                    tableProperties.setComment(StringUtils.trim(HiveDriverUtil.unicodeToStr(lineDataInternal.get(valueKey))));
                    find++;
                }

                if (!totalSize && (nameInternal.contains(KEY_TOTALSIZE) || nameInternal.contains(KEY_SPARK_TOTALSIZE))) {
                    tableProperties.setTotalSize(MapUtils.getLong(lineDataInternal, valueKey));
                    find++;
                    totalSize = true;
                }

                if (nameInternal.contains(KEY_TRANSIENT_LASTDDLTIME)) {
                    tableProperties.setTransientLastDdlTime(Integer.parseInt(MapUtils.getString(lineDataInternal, valueKey).trim()) * 1000L);
                    find++;
                }
                if (!numRows && (nameInternal.contains(KEY_NUMROWS) || nameInternal.contains(KEY_SPARK_NUMROWS))) {
                    tableProperties.setRows(MapUtils.getLong(lineDataInternal, valueKey));
                    find++;
                    numRows = true;
                }

                if (find == 4) {
                    break;
                }
            }
        }
    }
}
