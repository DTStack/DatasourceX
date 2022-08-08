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

package com.dtstack.dtcenter.common.loader.impala.metadata;

import com.dtstack.dtcenter.common.loader.common.utils.DateUtil;
import com.dtstack.dtcenter.common.loader.impala.ImpalaConnFactory;
import com.dtstack.dtcenter.common.loader.impala.metadata.entity.ImpalaTableEntity;
import com.dtstack.dtcenter.common.loader.impala.metadata.entity.MetadataImpalaEntity;
import com.dtstack.dtcenter.common.loader.rdbms.ConnFactory;
import com.dtstack.dtcenter.common.loader.rdbms.metadata.RdbmsMetaDataCollect;
import com.dtstack.dtcenter.loader.dto.metadata.entity.rdb.ColumnEntity;
import com.dtstack.dtcenter.loader.dto.metadata.entity.rdb.MetadataRdbEntity;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.dtstack.dtcenter.common.loader.impala.metadata.cons.ImpalaMetadataCons.ANALYZE_TABLE;
import static com.dtstack.dtcenter.common.loader.impala.metadata.cons.ImpalaMetadataCons.INVALID_META;
import static com.dtstack.dtcenter.common.loader.impala.metadata.cons.ImpalaMetadataCons.KEY_COLUMN_DATA_TYPE;
import static com.dtstack.dtcenter.common.loader.impala.metadata.cons.ImpalaMetadataCons.KEY_COL_CREATETIME;
import static com.dtstack.dtcenter.common.loader.impala.metadata.cons.ImpalaMetadataCons.KEY_COL_CREATE_TIME;
import static com.dtstack.dtcenter.common.loader.impala.metadata.cons.ImpalaMetadataCons.KEY_COL_LASTACCESSTIME;
import static com.dtstack.dtcenter.common.loader.impala.metadata.cons.ImpalaMetadataCons.KEY_COL_LAST_ACCESS_TIME;
import static com.dtstack.dtcenter.common.loader.impala.metadata.cons.ImpalaMetadataCons.KEY_COL_LOCATION;
import static com.dtstack.dtcenter.common.loader.impala.metadata.cons.ImpalaMetadataCons.KEY_COL_NAME;
import static com.dtstack.dtcenter.common.loader.impala.metadata.cons.ImpalaMetadataCons.KEY_COL_OUTPUTFORMAT;
import static com.dtstack.dtcenter.common.loader.impala.metadata.cons.ImpalaMetadataCons.KEY_COL_OWNER;
import static com.dtstack.dtcenter.common.loader.impala.metadata.cons.ImpalaMetadataCons.KEY_COL_TABLE_PARAMETERS;
import static com.dtstack.dtcenter.common.loader.impala.metadata.cons.ImpalaMetadataCons.KEY_COMMENT;
import static com.dtstack.dtcenter.common.loader.impala.metadata.cons.ImpalaMetadataCons.KEY_NUMROWS;
import static com.dtstack.dtcenter.common.loader.impala.metadata.cons.ImpalaMetadataCons.KEY_TOTALSIZE;
import static com.dtstack.dtcenter.common.loader.impala.metadata.cons.ImpalaMetadataCons.KEY_TRANSIENT_LASTDDLTIME;
import static com.dtstack.dtcenter.common.loader.impala.metadata.cons.ImpalaMetadataCons.ORC_FORMAT;
import static com.dtstack.dtcenter.common.loader.impala.metadata.cons.ImpalaMetadataCons.PARQUET_FORMAT;
import static com.dtstack.dtcenter.common.loader.impala.metadata.cons.ImpalaMetadataCons.PARTITION_INFORMATION;
import static com.dtstack.dtcenter.common.loader.impala.metadata.cons.ImpalaMetadataCons.SQL_QUERY_DATA;
import static com.dtstack.dtcenter.common.loader.impala.metadata.cons.ImpalaMetadataCons.SQL_SHOW_TABLES;
import static com.dtstack.dtcenter.common.loader.impala.metadata.cons.ImpalaMetadataCons.SQL_SWITCH_DATABASE;
import static com.dtstack.dtcenter.common.loader.impala.metadata.cons.ImpalaMetadataCons.TABLE_INFORMATION;
import static com.dtstack.dtcenter.common.loader.impala.metadata.cons.ImpalaMetadataCons.TEXT_FORMAT;
import static com.dtstack.dtcenter.common.loader.impala.metadata.cons.ImpalaMetadataCons.TYPE_ORC;
import static com.dtstack.dtcenter.common.loader.impala.metadata.cons.ImpalaMetadataCons.TYPE_PARQUET;
import static com.dtstack.dtcenter.common.loader.impala.metadata.cons.ImpalaMetadataCons.TYPE_TEXT;

/**
 * impala元数据收集器
 *
 * @author luming
 * @date 2022/6/7
 */
@Slf4j
public class ImpalaMetadataCollect extends RdbmsMetaDataCollect {

    @Override
    protected MetadataRdbEntity createMetadataRdbEntity() {
        MetadataImpalaEntity metadataImpalaEntity = new MetadataImpalaEntity();
        List<ColumnEntity> columnList = new ArrayList<>();
        List<ColumnEntity> partitionColumnList = new ArrayList<>();
        ImpalaTableEntity tableProperties = new ImpalaTableEntity();
        String tableName = (String) currentObject;
        analyzeTable(tableName);
        List<Map<String, String>> metaData;
        try {
            metaData = queryData(tableName);
        } catch (SQLException e) {
            throw new DtLoaderException("read metadata failed " + e.getMessage(), e);
        }
        Iterator<Map<String, String>> iterator = metaData.iterator();
        int metadataFlag = 0;
        while (iterator.hasNext()) {
            Map<String, String> metadata = iterator.next();
            String colNameInternal = metadata.get(KEY_COL_NAME);
            if (StringUtils.isBlank(colNameInternal)) continue;
            if (colNameInternal.startsWith("#")) {
                switch (colNameInternal) {
                    case PARTITION_INFORMATION:
                        metadataFlag = 1;
                        break;
                    case TABLE_INFORMATION:
                        metadataFlag = 2;
                        break;
                    default:
                        break;
                }
                continue;
            }
            switch (metadataFlag) {
                case 0:
                    columnList.add(parseColumn(metadata, columnList.size() + 1));
                    break;
                case 1:
                    partitionColumnList.add(parseColumn(metadata, partitionColumnList.size() + 1));
                    break;
                case 2:
                    parseTableProperties(metadata, tableProperties, iterator);
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
            metadataImpalaEntity.setPartitions(partitionColumnNames);
            tableProperties.setPartition(true);
        }
        metadataImpalaEntity.setTableProperties(tableProperties);
        metadataImpalaEntity.setColumns(columnList);
        metadataImpalaEntity.setPartitionColumns(partitionColumnList);
        return metadataImpalaEntity;
    }

    @Override
    protected ConnFactory getConnFactory() {
        return new ImpalaConnFactory();
    }


    private void parseTableProperties(Map<String, String> metadata, ImpalaTableEntity tableProperties, Iterator<Map<String, String>> it) {
        String name = metadata.get(KEY_COL_NAME);
        if (name.contains(KEY_COL_OWNER)) {
            tableProperties.setOwner(StringUtils.trim(metadata.get(KEY_COLUMN_DATA_TYPE)));
        }
        if (name.contains(KEY_COL_LOCATION)) {
            tableProperties.setLocation(StringUtils.trim(metadata.get(KEY_COLUMN_DATA_TYPE)));
        }
        if (name.contains(KEY_COL_CREATETIME) || name.contains(KEY_COL_CREATE_TIME)) {
            tableProperties.setCreateTime(DateUtil.stringToLong(StringUtils.trim(metadata.get(KEY_COLUMN_DATA_TYPE))));
        }
        if (name.contains(KEY_COL_LASTACCESSTIME) || name.contains(KEY_COL_LAST_ACCESS_TIME)) {
            String str = StringUtils.trim(metadata.get(KEY_COLUMN_DATA_TYPE));
            tableProperties.setLastAccessTime("UNKNOWN".equals(str) ? null : Long.parseLong(str) / 1000);
        }
        boolean isHbaseTable = false;
        boolean isKuduTable = false;
        if (name.contains(KEY_COL_TABLE_PARAMETERS)) {
            int find = 0;
            boolean totalSize = false;
            boolean numRows = false;
            while (it.hasNext()) {
                metadata = it.next();
                String nameInternal = metadata.get(KEY_COL_NAME);
                String typeInternal = metadata.get(KEY_COLUMN_DATA_TYPE);
                String valueKey = KEY_COLUMN_DATA_TYPE;
                if (StringUtils.isBlank(nameInternal)) {
                    if (StringUtils.isBlank(typeInternal)) {
                        continue;
                    } else {
                        nameInternal = typeInternal;
                        valueKey = KEY_COMMENT;
                    }
                }
                nameInternal = nameInternal.trim();
                if (StringUtils.isNotBlank(nameInternal)) {
                    if (nameInternal.toLowerCase().contains("hbase"))
                        isHbaseTable = true;
                    else if (nameInternal.toLowerCase().contains("kudu"))
                        isKuduTable = true;
                }
                if (nameInternal.contains(KEY_COMMENT)) {
                    tableProperties.setComment(StringUtils.trim(unicodeToStr(metadata.get(valueKey))));
                    find++;
                }

                if (!totalSize && (nameInternal.contains(KEY_TOTALSIZE))) {
                    tableProperties.setTotalSize(MapUtils.getLong(metadata, valueKey));
                    find++;
                    totalSize = true;
                }

                if (nameInternal.contains(KEY_TRANSIENT_LASTDDLTIME)) {
                    tableProperties.setTransientLastDdlTime(MapUtils.getLong(metadata, valueKey) * 1000);
                    find++;
                }

                if (!numRows && (nameInternal.contains(KEY_NUMROWS))) {
                    tableProperties.setRows(MapUtils.getLong(metadata, valueKey));
                    find++;
                    numRows = true;
                }
                if (nameInternal.contains(KEY_COL_OUTPUTFORMAT)) {
                    String storedClass = metadata.get(KEY_COLUMN_DATA_TYPE);
                    tableProperties.setStoreType(getStoredType(storedClass));
                    find++;
                }
                if (find == 5) {
                    break;
                }
            }
            if (isHbaseTable) {
                tableProperties.setTableType("hbase");
            } else if (isKuduTable) {
                tableProperties.setTableType("kudu");
            } else {
                tableProperties.setTableType("hdfs");
            }
        }
    }

    /**
     * Unicode 编码转字符串
     *
     * @param string 支持 Unicode 编码和普通字符混合的字符串
     * @return 解码后的字符串
     */
    public static String unicodeToStr(String string) {
        String prefix = "\\u";
        if (string == null || !string.contains(prefix)) {
            // 传入字符串为空或不包含 Unicode 编码返回原内容
            return string;
        }

        StringBuilder value = new StringBuilder(string.length() >> 2);
        String[] strings = string.split("\\\\u");
        String hex, mix;
        char hexChar;
        int ascii, n;

        if (strings[0].length() > 0) {
            // 处理开头的普通字符串
            value.append(strings[0]);
        }

        try {
            for (int i = 1; i < strings.length; i++) {
                hex = strings[i];
                if (hex.length() > 3) {
                    mix = "";
                    if (hex.length() > 4) {
                        // 处理 Unicode 编码符号后面的普通字符串
                        mix = hex.substring(4);
                    }
                    hex = hex.substring(0, 4);

                    try {
                        Integer.parseInt(hex, 16);
                    } catch (Exception e) {
                        // 不能将当前 16 进制字符串正常转换为 10 进制数字，拼接原内容后跳出
                        value.append(prefix).append(strings[i]);
                        continue;
                    }

                    ascii = 0;
                    for (int j = 0; j < hex.length(); j++) {
                        hexChar = hex.charAt(j);
                        // 将 Unicode 编码中的 16 进制数字逐个转为 10 进制
                        n = Integer.parseInt(String.valueOf(hexChar), 16);
                        // 转换为 ASCII 码
                        ascii += n * ((int) Math.pow(16, (hex.length() - j - 1)));
                    }

                    // 拼接解码内容
                    value.append((char) ascii).append(mix);
                } else {
                    // 不转换特殊长度的 Unicode 编码
                    value.append(prefix).append(hex);
                }
            }
        } catch (Exception e) {
            // Unicode 编码格式有误，解码失败
            return null;
        }

        return value.toString();
    }

    private ColumnEntity parseColumn(Map<String, String> lineDataInternal, int index) {
        ColumnEntity columnEntity = new ColumnEntity();
        String typeInternal = lineDataInternal.get(KEY_COLUMN_DATA_TYPE);
        String commentInternal = lineDataInternal.get(KEY_COMMENT);
        String colNameInternal = lineDataInternal.get(KEY_COL_NAME);
        columnEntity.setType(typeInternal);
        if (typeInternal.contains("(") && typeInternal.contains(")")) {
            int precisionEndPosition = !typeInternal.contains(",") ? typeInternal.indexOf(")") : typeInternal.indexOf(",");
            String precision = StringUtils.substring(typeInternal, typeInternal.indexOf("(") + 1, precisionEndPosition);
            columnEntity.setDigital(Integer.parseInt(precision));
            if (precisionEndPosition < typeInternal.length() - 1) {
                columnEntity.setDigital(Integer.parseInt(StringUtils.substring(typeInternal, precisionEndPosition + 1, typeInternal.length() - 1)));
            }
        }
        columnEntity.setComment(commentInternal);
        columnEntity.setName(colNameInternal);
        columnEntity.setIndex(index);
        return columnEntity;
    }

    private String getStoredType(String storedClass) {
        if (storedClass.endsWith(TEXT_FORMAT)) {
            return TYPE_TEXT;
        } else if (storedClass.endsWith(ORC_FORMAT)) {
            return TYPE_ORC;
        } else if (storedClass.endsWith(PARQUET_FORMAT)) {
            return TYPE_PARQUET;
        } else if (storedClass.contains("null")) {
            return null;
        } else {
            return storedClass;
        }
    }

    protected String quote(String name) {
        if (StringUtils.isBlank(name)) {
            return name;
        }
        return String.format("`%s`", name);
    }

    /**
     * 刷新元数据信息
     */
    private void analyzeTable(String tableName) {
        try {
            String sql = String.format(ANALYZE_TABLE, tableName);
            log.info("execute analyze sql: {}", sql);
            long l = System.currentTimeMillis();
            Statement statement = connection.createStatement();
            statement.execute(sql);
            log.info("execute analyze sql: {} , time: {}", sql, System.currentTimeMillis() - l);
        } catch (Exception e) {
            log.warn("execute analyzeTable for table: {} failed", tableName, e);
        }
    }


    /**
     * 查询表元数据
     *
     * @param table
     * @return
     * @throws SQLException
     */
    private List<Map<String, String>> queryData(String table) throws SQLException {
        for (int time = 0; time < 3; time++) {
            try {
                if (StringUtils.isNotBlank(table)) statement.execute(String.format(INVALID_META, quote(table)));
                ResultSet rs = statement.executeQuery(String.format(SQL_QUERY_DATA, quote(table)));
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
                log.warn("query data error", e);
                if (time == 2) {
                    throw e;
                }
                resetConnection();
            }
        }
        throw new RuntimeException("queryData error");

    }

    /**
     * 重新获取connection
     *
     * @throws SQLException sql异常
     */
    private void resetConnection() throws SQLException {
        try {
            connection.close();
        } catch (Exception e) {
            log.warn("connection is not valid and an exception occurred while closing", e);
        }
        try {
            connection = connFactory.getConn(sourceDTO);
        } catch (Exception e) {
            log.error("Failed to get connection again, " + ExceptionUtils.getRootCauseMessage(e), e);
        }
        statement = connection.createStatement();
        switchDataBase();
    }

    @Override
    public void switchDataBase() throws SQLException {
        statement.execute(String.format(SQL_SWITCH_DATABASE, quote(currentDatabase)));
    }

    @Override
    public List<Object> showTables() {
        List<Object> tables = new ArrayList<>();
        try (ResultSet rs = statement.executeQuery(SQL_SHOW_TABLES)) {
            int pos = rs.getMetaData().getColumnCount() == 1 ? 1 : 2;
            while (rs.next()) {
                tables.add(rs.getString(pos));
            }
        } catch (SQLException e) {
            throw new DtLoaderException("", e);
        }
        return tables;
    }
}
