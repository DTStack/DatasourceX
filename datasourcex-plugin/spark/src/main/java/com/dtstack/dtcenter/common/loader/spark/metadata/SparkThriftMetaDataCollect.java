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

package com.dtstack.dtcenter.common.loader.spark.metadata;

import com.dtstack.dtcenter.common.loader.common.utils.DateUtil;
import com.dtstack.dtcenter.common.loader.rdbms.ConnFactory;
import com.dtstack.dtcenter.common.loader.rdbms.metadata.RdbmsMetaDataCollect;
import com.dtstack.dtcenter.common.loader.spark.SparkConnFactory;
import com.dtstack.dtcenter.common.loader.spark.metadata.constants.StMetaDataCons;
import com.dtstack.dtcenter.common.loader.spark.metadata.utils.HiveOperatorUtils;
import com.dtstack.dtcenter.loader.dto.metadata.entity.rdb.ColumnEntity;
import com.dtstack.dtcenter.loader.dto.metadata.entity.rdb.MetadataRdbEntity;
import com.dtstack.dtcenter.loader.dto.metadata.entity.sparkthrift.MetadatastEntity;
import com.dtstack.dtcenter.loader.dto.metadata.entity.sparkthrift.StTableEntity;
import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Slf4j
public class SparkThriftMetaDataCollect extends RdbmsMetaDataCollect {

    Pattern toDatePattern = Pattern.compile("(?i)(\tComment:(?<value>(.*?))\t)", Pattern.DOTALL | Pattern.MULTILINE);

    ISourceDTO sourceDTO;

    /**
     * hive 配置信息
     */
    protected Map<String, Object> hadoopConfig;

    @Override
    protected MetadataRdbEntity createMetadataRdbEntity() {
        MetadatastEntity metadatastEntity = new MetadatastEntity();
        List<ColumnEntity> columnList = new ArrayList<>();
        List<ColumnEntity> partitionColumnList = new ArrayList<>();
        StTableEntity tableProperties = new StTableEntity();
        String tableName = (String) currentObject;
        List<Map<String, String>> metaData;
        try {
            preParseTable(tableName);
            metaData = queryData(tableName);
        } catch (SQLException | IOException e) {
            throw new DtLoaderException("read metadata failed");
        }
        Iterator<Map<String, String>> it = metaData.iterator();
        int metaDataFlag = 0;
        while (it.hasNext()) {
            Map<String, String> lineDataInternal = it.next();
            String colNameInternal = lineDataInternal.get(StMetaDataCons.KEY_COL_NAME);
            if (StringUtils.isBlank(colNameInternal)) {
                continue;
            }
            if (colNameInternal.startsWith("#")) {
                colNameInternal = StringUtils.trim(colNameInternal);
                switch (colNameInternal) {
                    case StMetaDataCons.PARTITION_INFORMATION:
                        metaDataFlag = 1;
                        break;
                    case StMetaDataCons.TABLE_INFORMATION:
                        metaDataFlag = 2;
                        break;
                    // just to get storeType from OutputFormat
                    case StMetaDataCons.STORAGE_INFORMATION:
                        metaDataFlag = 2;
                        break;
                    case StMetaDataCons.KEY_RESULTSET_COL_NAME:
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
            metadatastEntity.setPartitions(partitionColumnNames);
        }
        metadatastEntity.setTableProperties(tableProperties);
        metadatastEntity.setPartitionColumns(partitionColumnList);
        metadatastEntity.setColumns(columnList);
        return metadatastEntity;
    }

    /**
     * 在解析表信息之前执行的操作
     */
    private void preParseTable(String tableName) throws IOException {
        List<String> partitionColumnList = new ArrayList<>();
        try {
            List<Map<String, String>> metaData = queryData(tableName);
            Iterator<Map<String, String>> it = metaData.iterator();
            int metaDataFlag = 0;
            while (it.hasNext()) {
                Map<String, String> lineDataInternal = it.next();
                String colNameInternal = lineDataInternal.get(StMetaDataCons.KEY_COL_NAME);
                if (StringUtils.isBlank(colNameInternal)) {
                    continue;
                }
                if (colNameInternal.startsWith("#")) {
                    colNameInternal = StringUtils.trim(colNameInternal);
                    switch (colNameInternal) {
                        case StMetaDataCons.PARTITION_INFORMATION:
                            metaDataFlag = 1;
                            break;
                        case StMetaDataCons.TABLE_INFORMATION:
                            // just to get storeType from OutputFormat
                        case StMetaDataCons.STORAGE_INFORMATION:
                            metaDataFlag = 2;
                            break;
                        default:
                            break;
                    }
                    continue;
                }
                if (metaDataFlag == 1) {
                    partitionColumnList.add(lineDataInternal.get(StMetaDataCons.KEY_COL_NAME));
                }
            }
            analyzeTable(tableName, partitionColumnList);
        } catch (Exception e) {
            throw new IOException("get partiitonCol failed " + e.getMessage(), e);
        }
    }

    protected String quote(String name) {
        if (StringUtils.isBlank(name)) {
            return name;
        }
        return String.format("`%s`", name);
    }

    protected List<Map<String, String>> queryData(String table) throws SQLException {
        for (int time = 0; time < 3; time++) {
            List<Map<String, String>> data = new ArrayList<>();
            try (ResultSet rs = statement.executeQuery(String.format(StMetaDataCons.SQL_QUERY_DATA, quote(table)))) {
                ResultSetMetaData metaData = rs.getMetaData();
                int columnCount = metaData.getColumnCount();
                List<String> columnNames = new ArrayList<>(columnCount);
                for (int i = 0; i < columnCount; i++) {
                    columnNames.add(metaData.getColumnName(i + 1));
                }

                while (rs.next()) {
                    Map<String, String> lineData = new HashMap<>(Math.max((int) (columnCount / .75f) + 1, 16));
                    for (String columnName : columnNames) {
                        lineData.put(columnName, rs.getString(columnName));
                    }
                    data.add(lineData);
                }

                Map<String, String> tableComment = getTableComment(table);
                if (MapUtils.isNotEmpty(tableComment)) {
                    for (int index = 0; index < data.size(); index++) {
                        Map<String, String> map = data.get(index);
                        String colNameInternal = map.get(StMetaDataCons.KEY_COL_NAME);

                        if (StringUtils.isBlank(colNameInternal)) {
                            continue;
                        }
                        if (colNameInternal.startsWith("#")) {
                            colNameInternal = StringUtils.trim(colNameInternal);
                            //comment信息应该放在 TABLE_INFORMATION 后面
                            if (StMetaDataCons.TABLE_INFORMATION.equals(colNameInternal)) {
                                data.add(++index, tableComment);
                            }
                        }
                    }
                }
                return data;
            } catch (Exception e) {
                log.error("query data error", e);
                if (time == 2) {
                    throw e;
                }
                resetConnection(sourceDTO);
            }
        }

        throw new RuntimeException("queryData error");
    }

    private ColumnEntity parseColumn(Map<String, String> lineDataInternal, int index) {
        ColumnEntity stColumnEntity = new ColumnEntity();
        String dataTypeInternal = lineDataInternal.get(StMetaDataCons.KEY_DATA_TYPE);
        String commentInternal = lineDataInternal.get(StMetaDataCons.KEY_COMMENT);
        String colNameInternal = lineDataInternal.get(StMetaDataCons.KEY_COL_NAME);
        stColumnEntity.setType(dataTypeInternal);
        stColumnEntity.setComment(commentInternal);
        stColumnEntity.setName(colNameInternal);
        stColumnEntity.setIndex(index);
        return stColumnEntity;
    }

    private void parseTableProperties(Map<String, String> lineDataInternal, StTableEntity tableProperties, Iterator<Map<String, String>> it) {
        String name = lineDataInternal.get(StMetaDataCons.KEY_COL_NAME);

        if (StMetaDataCons.KEY_COL_LOCATION.equals(name) || StMetaDataCons.KEY_COL_LOCATION_IS.equals(name)) {
            tableProperties.setLocation(StringUtils.trim(lineDataInternal.get(StMetaDataCons.KEY_DATA_TYPE)));
        }
        if (StMetaDataCons.KEY_COL_CREATED.equals(name) || StMetaDataCons.KEY_COL_CREATED_TIME.equals(name)
                || StMetaDataCons.KEY_COL_CREATE_TIME.equals(name)) {
            tableProperties.setCreateTime(DateUtil.stringToLong(StringUtils.trim(lineDataInternal.get(StMetaDataCons.KEY_DATA_TYPE))));
        }
        if (StMetaDataCons.KEY_COL_LASTACCESS.equals(name) || StMetaDataCons.KEY_COL_LAST_ACCESS_TIME.equals(name)) {
            tableProperties.setLastAccessTime(DateUtil.stringToLong(StringUtils.trim(lineDataInternal.get(StMetaDataCons.KEY_DATA_TYPE))));
        }
        if (StMetaDataCons.KEY_COL_OUTPUTFORMAT.equals(name) || StMetaDataCons.KEY_COL_OUTPUTFORMAT_IS.equals(name)) {
            String storedClass = lineDataInternal.get(StMetaDataCons.KEY_DATA_TYPE);
            tableProperties.setStoreType(HiveOperatorUtils.getStoredType(storedClass));
        }

        if (StMetaDataCons.KEY_COL_COMMENT.equals(name)) {
            String comment = lineDataInternal.get(StMetaDataCons.KEY_DATA_TYPE);
            tableProperties.setComment(comment);
        }

        //Statistics: like sizeInBytes=17968503, rowCount=78444, isBroadcastable=false
        if (StMetaDataCons.KEY_STATISTICS_COLON.equals(name) || StMetaDataCons.KEY_STATISTICS.equals(name)) {
            String statistics = lineDataInternal.get(StMetaDataCons.KEY_DATA_TYPE);
            Map<String, String> map = HiveOperatorUtils.parseStatisticsColonToMap(statistics);
            if (MapUtils.isNotEmpty(map)) {
                if (map.containsKey(StMetaDataCons.KEY_STATISTICS_SIZEINBYTES)) {
                    tableProperties.setTotalSize(Long.parseLong(map.get(StMetaDataCons.KEY_STATISTICS_SIZEINBYTES)));
                }

                if (map.containsKey(StMetaDataCons.KEY_STATISTICS_ROWCOUNT)) {
                    tableProperties.setRows(Long.parseLong(map.get(StMetaDataCons.KEY_STATISTICS_ROWCOUNT)));
                }
            }
        }

        if (StMetaDataCons.KEY_COL_TABLE_PROPERTIES.equals(name)) {
            String tableInfo = lineDataInternal.get(StMetaDataCons.KEY_DATA_TYPE);
            //tableInfo like this : [numFiles=1, transient_lastDdlTime=1614167867, totalSize=414]
            Map<String, String> map = HiveOperatorUtils.parseToMap(tableInfo);
            if (MapUtils.isNotEmpty(map)) {
                if (map.containsKey(StMetaDataCons.KEY_COMMENT)) {
                    tableProperties.setComment(map.get(StMetaDataCons.KEY_COMMENT));
                }

                if (map.containsKey(StMetaDataCons.KEY_TOTALSIZE)) {
                    tableProperties.setTotalSize(Long.parseLong(map.get(StMetaDataCons.KEY_TOTALSIZE)));
                }

                if (map.containsKey(StMetaDataCons.KEY_TRANSIENT_LASTDDLTIME)) {
                    tableProperties.setTransientLastDdlTime(Long.parseLong(map.get(StMetaDataCons.KEY_TRANSIENT_LASTDDLTIME)) * 1000L);
                }
            }
        }

        if (StMetaDataCons.KEY_COL_TABLE_PARAMETERS.equals(name)) {
            while (it.hasNext()) {
                lineDataInternal = it.next();
                String nameInternal = lineDataInternal.get(StMetaDataCons.KEY_COL_NAME);
                if (StringUtils.isEmpty(nameInternal)) {
                    break;
                }
                nameInternal = nameInternal.trim();
                if (StMetaDataCons.KEY_COMMENT.equals(nameInternal)) {
                    tableProperties.setComment(StringUtils.trim(lineDataInternal.get(StMetaDataCons.KEY_DATA_TYPE)));
                }

                if (StMetaDataCons.KEY_TOTALSIZE.equals(nameInternal)) {
                    tableProperties.setTotalSize(Long.parseLong(StringUtils.trim(lineDataInternal.get(StMetaDataCons.KEY_DATA_TYPE))));
                }

                if (StMetaDataCons.KEY_TRANSIENT_LASTDDLTIME.equals(nameInternal)) {
                    tableProperties.setTransientLastDdlTime(Long.parseLong(StringUtils.trim(lineDataInternal.get(StMetaDataCons.KEY_DATA_TYPE))) * 1000L);
                }
            }
        }
    }

    /**
     * 执行 analyzeTable 刷新元数据信息
     * spark 去除 no scan 否则数据行数无法刷新出来
     */
    private void analyzeTable(String tableName, List<String> partitionColumnList) {
        try {
            String partiiton = "";
            if (CollectionUtils.isNotEmpty(partitionColumnList)) {
                partiiton = "partition(" + String.join(",", partitionColumnList) + ")";
            }

            String sql = String.format(StMetaDataCons.ANALYZE_TABLE, quote(tableName), quote(partiiton));
            log.info("execute analyze sql {}", sql);
            long l = System.currentTimeMillis();
            Statement statement = connection.createStatement();
            statement.execute(sql);
            log.info("execute analyze sql {} time {}", sql, System.currentTimeMillis() - l);
        } catch (Exception e) {
            log.warn("execute analyzeTable for " + tableName + " failed", e);
        }
    }

    /**
     * 表注释无法通过desc formatted获取到
     * 通过desc extended sql获取到之后 根据正则解析获取comment值
     *
     * @param table
     * @return
     * @throws SQLException
     */
    private Map<String, String> getTableComment(String table) throws SQLException {
        String detailInformation = null;
        try (ResultSet rs = statement.executeQuery(String.format(StMetaDataCons.SQL_QUERY_DATA_EXTENDED, quote(table)))) {
            while (rs.next()) {
                String string = rs.getString(StMetaDataCons.KEY_COL_NAME);
                if (StringUtils.isNotBlank(string) && StMetaDataCons.TABLE_INFORMATION.equals(StringUtils.trim(string))) {
                    detailInformation = rs.getString(StMetaDataCons.KEY_DATA_TYPE);
                }
            }
        }

        if (StringUtils.isBlank(detailInformation)) {
            return null;
        }
        Matcher matcher = toDatePattern.matcher(detailInformation);
        HashMap<String, String> comment = new HashMap<>(8);

        if (matcher.find()) {
            String value = matcher.group("value");
            comment.put(StMetaDataCons.KEY_COL_NAME, StMetaDataCons.KEY_COL_COMMENT);
            comment.put(StMetaDataCons.KEY_DATA_TYPE, value);
        }
        return comment;
    }

    /**
     * 获取新的connection
     *
     * @throws SQLException sql异常
     */
    private void resetConnection(ISourceDTO sourceDTO) throws SQLException {
        try {
            connection.close();
        } catch (Exception e) {
            log.warn("connection is not valid and an exception occurred while closing", e);
        }
        connection = getConnection(sourceDTO);
        statement = connection.createStatement();
        switchDataBase();
    }


    public Connection getConnection(ISourceDTO sourceDTO) {
        SparkConnFactory sparkConnFactory = new SparkConnFactory();
        Connection connection;
        try {
            connection = sparkConnFactory.getConn(sourceDTO);
        } catch (Exception e) {
            throw new DtLoaderException("get connection failed");
        }
        return connection;
    }

    @Override
    public List<Object> showTables() {
        List<Object> tables = new ArrayList<>();
        try (ResultSet rs = statement.executeQuery(StMetaDataCons.SQL_SHOW_TABLES)) {
            int pos = rs.getMetaData().getColumnCount() == 1 ? 1 : 2;
            while (rs.next()) {
                tables.add(rs.getString(pos));
            }
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
        return tables;
    }

    @Override
    public void switchDataBase() throws SQLException {
        statement.execute(String.format(StMetaDataCons.SQL_SWITCH_DATABASE, quote(currentDatabase)));
    }

    @Override
    protected ConnFactory getConnFactory() {
        return new SparkConnFactory();
    }
}
