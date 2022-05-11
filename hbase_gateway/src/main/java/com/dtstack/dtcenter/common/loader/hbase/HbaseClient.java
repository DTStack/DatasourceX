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

package com.dtstack.dtcenter.common.loader.hbase;

import com.dtstack.dtcenter.common.loader.common.nosql.AbsNoSqlClient;
import com.dtstack.dtcenter.loader.dto.ColumnMetaDTO;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.filter.RowFilter;
import com.dtstack.dtcenter.loader.dto.filter.TimestampFilter;
import com.dtstack.dtcenter.loader.dto.source.HbaseSourceDTO;
import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;
import com.dtstack.dtcenter.loader.enums.CompareOp;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 19:59 2020/2/27
 * @Description：Hbase 客户端
 */
@Slf4j
public class HbaseClient<T> extends AbsNoSqlClient<T> {
    private HbaseConnFactory connFactory = new HbaseConnFactory();

    private static final String ROWKEY = "rowkey";

    private static final String FAMILY_QUALIFIER = "%s:%s";

    private static final String TIMESTAMP = "timestamp";

    @Override
    public Boolean testCon(ISourceDTO iSource) {
        return connFactory.testConn(iSource);
    }

    @Override
    public List<String> getTableList(ISourceDTO iSource, SqlQueryDTO queryDTO)  {
        HbaseSourceDTO hbaseSourceDTO = (HbaseSourceDTO) iSource;
        Connection hConn = null;
        Admin admin = null;
        List<String> tableList = new ArrayList<>();
        try {
            hConn = HbaseConnFactory.getHbaseConn(hbaseSourceDTO, queryDTO);
            admin = hConn.getAdmin();
            TableName[] tableNames = admin.listTableNames();
            if (tableNames != null) {
                for (TableName tableName : tableNames) {
                    tableList.add(tableName.getNameAsString());
                }
            }
        } catch (IOException e) {
            throw new DtLoaderException(String.format("get hbase table list exception,%s", e.getMessage()), e);
        } finally {
            closeAdmin(admin);
            closeConnection(hConn,hbaseSourceDTO);
        }
        return tableList;
    }

    private static void closeConnection(Connection hConn, HbaseSourceDTO hbaseSourceDTO) {
        if ((hbaseSourceDTO.getPoolConfig() == null || MapUtils.isNotEmpty(hbaseSourceDTO.getKerberosConfig())) && hConn != null) {
            try {
                hConn.close();
            } catch (IOException e) {
                log.error("hbase Close connection exception", e);
            }
        }
    }

    private static void closeAdmin(Admin admin) {
        if (admin != null) {
            try {
                admin.close();
            } catch (IOException e) {
                log.error("hbase Close connection exception", e);
            }
        }
    }

    @Override
    public List<ColumnMetaDTO> getColumnMetaData(ISourceDTO iSource, SqlQueryDTO queryDTO)  {
        HbaseSourceDTO hbaseSourceDTO = (HbaseSourceDTO) iSource;
        Connection hConn = null;
        Table tb = null;
        List<ColumnMetaDTO> cfList = new ArrayList<>();
        try {
            hConn = HbaseConnFactory.getHbaseConn(hbaseSourceDTO, queryDTO);
            TableName tableName = TableName.valueOf(queryDTO.getTableName());
            tb = hConn.getTable(tableName);
            HTableDescriptor hTableDescriptor = tb.getTableDescriptor();
            HColumnDescriptor[] columnDescriptors = hTableDescriptor.getColumnFamilies();
            for(HColumnDescriptor columnDescriptor: columnDescriptors) {
                ColumnMetaDTO columnMetaDTO = new ColumnMetaDTO();
                columnMetaDTO.setKey(columnDescriptor.getNameAsString());
                cfList.add(columnMetaDTO);
            }
        } catch (IOException e) {
            throw new DtLoaderException(String.format("hbase list column families error,%s", e.getMessage()), e);
        } finally {
            closeTable(tb);
            closeConnection(hConn,hbaseSourceDTO);
        }
        return cfList;
    }

    @Override
    public List<Map<String, Object>> executeQuery(ISourceDTO source, SqlQueryDTO queryDTO) {
        HbaseSourceDTO hbaseSourceDTO = (HbaseSourceDTO) source;
        Connection connection = null;
        Table table = null;
        ResultScanner rs = null;
        List<Result> results = Lists.newArrayList();
        List<Map<String, Object>> executeResult = Lists.newArrayList();
        try {
            //获取hbase连接
            connection = HbaseConnFactory.getHbaseConn(hbaseSourceDTO, queryDTO);
            //获取hbase扫描列，格式 - 列族:列名
            List<String> columns = queryDTO.getColumns();
            //获取hbase自定义查询的过滤器
            List<com.dtstack.dtcenter.loader.dto.filter.Filter> hbaseFilter = queryDTO.getHbaseFilter();
            TableName tableName = TableName.valueOf(queryDTO.getTableName());
            table = connection.getTable(tableName);
            List<Filter> filterList = Lists.newArrayList();
            Scan scan = new Scan();
            if (columns != null) {
                for (String column : columns) {
                    String[] familyAndQualifier = column.split(":");
                    if (familyAndQualifier.length < 2) {
                        continue;
                    }
                    scan.addColumn(Bytes.toBytes(familyAndQualifier[0]), Bytes.toBytes(familyAndQualifier[1]));
                }
            }

            boolean isAccurateQuery = false;
            if (hbaseFilter != null && hbaseFilter.size() > 0) {
                for (com.dtstack.dtcenter.loader.dto.filter.Filter filter : hbaseFilter){
                    if (getAccurateQuery(table, results, filter)) {
                        isAccurateQuery = true;
                        break;
                    }
                    // 针对时间戳过滤器进行封装
                    if ("TimestampFilter".equals(filter.getClass().getSimpleName()) && filter instanceof TimestampFilter) {
                        TimestampFilter timestampFilter = (TimestampFilter) filter;
                        fillTimestampFilter(scan, timestampFilter);
                        continue;
                    }
                    //将core包下的filter转换成hbase包下的filter
                    Filter transFilter = FilterType.get(filter);
                    filterList.add(transFilter);
                }
                FilterList filters = new FilterList(filterList);
                scan.setFilter(filters);
            }
            if(!isAccurateQuery){
                rs = table.getScanner(scan);
                for (Result r : rs) {
                    results.add(r);
                }
            }

        } catch (Exception e){
            throw new DtLoaderException(String.format("Failed to execute hbase customization,%s", e.getMessage()), e);
        } finally {
            if (hbaseSourceDTO.getPoolConfig() == null || MapUtils.isNotEmpty(hbaseSourceDTO.getKerberosConfig())) {
                close(rs, table, connection);
            } else {
                close(rs, table, null);
            }
        }

        //理解为一行记录
        for (Result result : results) {
            List<Cell> cells = result.listCells();
            if (CollectionUtils.isEmpty(cells)) {
                continue;
            }
            long timestamp = 0L;
            HashMap<String, Object> row = Maps.newHashMap();
            for (Cell cell : cells){
                row.put(ROWKEY, Bytes.toString(cell.getRowArray(), cell.getRowOffset(),cell.getRowLength()));
                String family = Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(),cell.getFamilyLength());
                String qualifier = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(),cell.getQualifierLength());
                String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(),cell.getValueLength());
                row.put(String.format(FAMILY_QUALIFIER, family, qualifier), value);
                //取到最新变动的时间
                if (cell.getTimestamp() > timestamp) {
                    timestamp = cell.getTimestamp();
                }
            }
            row.put(TIMESTAMP, timestamp);
            executeResult.add(row);
        }

        return executeResult;
    }

    /**
     * 填充hbase自定义查询时间戳过滤参数
     * @param scan scan对象
     * @param timestampFilter 时间戳过滤器
     */
    private void fillTimestampFilter(Scan scan, TimestampFilter timestampFilter) throws IOException {
        CompareOp compareOp = timestampFilter.getCompareOp();
        Long comparator = timestampFilter.getComparator();
        if (Objects.isNull(comparator)) {
            return;
        }
        switch (compareOp) {
            case LESS:
                scan.setTimeRange(TimeRange.INITIAL_MIN_TIMESTAMP, comparator);
                break;
            case EQUAL:
                scan.setTimeStamp(comparator);
                break;
            case GREATER:
                scan.setTimeRange(comparator + 1, TimeRange.INITIAL_MAX_TIMESTAMP);
                break;
            case LESS_OR_EQUAL:
                scan.setTimeRange(TimeRange.INITIAL_MIN_TIMESTAMP, comparator + 1);
                break;
            case GREATER_OR_EQUAL:
                scan.setTimeRange(comparator, TimeRange.INITIAL_MAX_TIMESTAMP);
                break;
            default:
        }
    }

    private boolean getAccurateQuery(Table table, List<Result> results, com.dtstack.dtcenter.loader.dto.filter.Filter filter) throws IOException {
        if (filter instanceof RowFilter) {
            RowFilter rowFilterFilter = (RowFilter) filter;
            if (rowFilterFilter.getCompareOp().equals(CompareOp.EQUAL)) {
                Get get = new Get(rowFilterFilter.getComparator().getValue());
                Result r = table.get(get);
                results.add(r);
                return true;
            }
        }
        return false;
    }

    @Override
    public List<List<Object>> getPreview(ISourceDTO source, SqlQueryDTO queryDTO) {
        HbaseSourceDTO hbaseSourceDTO = (HbaseSourceDTO) source;
        Connection connection = null;
        Table table = null;
        ResultScanner rs = null;
        List<Result> results = Lists.newArrayList();
        List<List<Object>> executeResult = Lists.newArrayList();
        try {
            //获取hbase连接
            connection = HbaseConnFactory.getHbaseConn(hbaseSourceDTO, queryDTO);
            TableName tableName = TableName.valueOf(queryDTO.getTableName());
            table = connection.getTable(tableName);
            Scan scan = new Scan();
            //数据预览限制返回条数
            scan.setMaxResultSize(queryDTO.getPreviewNum());
            scan.setFilter(new PageFilter(queryDTO.getPreviewNum()));
            rs = table.getScanner(scan);
            for (Result r : rs) {
                results.add(r);
            }
        } catch (Exception e){
            throw new DtLoaderException(String.format("Data preview failed,%s", e.getMessage()), e);
        } finally {
            if (hbaseSourceDTO.getPoolConfig() == null || MapUtils.isNotEmpty(hbaseSourceDTO.getKerberosConfig())) {
                close(rs, table, connection);
            } else {
                close(rs, table, null);
            }
        }

        //理解为一行记录
        for (Result result : results) {
            List<Cell> cells = result.listCells();
            long timestamp = 0L;
            HashMap<String, Object> row = Maps.newHashMap();
            for (Cell cell : cells){
                row.put(ROWKEY, Bytes.toString(cell.getRowArray(), cell.getRowOffset(),cell.getRowLength()));
                String family = Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(),cell.getFamilyLength());
                String qualifier = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(),cell.getQualifierLength());
                String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(),cell.getValueLength());
                row.put(String.format(FAMILY_QUALIFIER, family, qualifier), value);
                //取到最新变动的时间
                if (cell.getTimestamp() > timestamp) {
                    timestamp = cell.getTimestamp();
                }
            }
            row.put(TIMESTAMP, timestamp);
            executeResult.add(Lists.newArrayList(row));
        }
        return executeResult;
    }

    public static void closeTable(Table table) {
        if(table != null) {
            try {
                table.close();
            } catch (IOException e) {
                throw new DtLoaderException(String.format("hbase can not close table error,%s", e.getMessage()), e);
            }
        }
    }

    private void close(Closeable... closeables) {
        try {
            if (Objects.nonNull(closeables)) {
                for (Closeable closeable : closeables) {
                    if (Objects.nonNull(closeable)) {
                        closeable.close();
                    }
                }
            }
        } catch (Exception e) {
            throw new DtLoaderException(String.format("hbase can not close table error,%s", e.getMessage()), e);
        }
    }
}
