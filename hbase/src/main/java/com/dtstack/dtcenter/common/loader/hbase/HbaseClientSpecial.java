package com.dtstack.dtcenter.common.loader.hbase;

import com.dtstack.dtcenter.loader.client.IHbase;
import com.dtstack.dtcenter.loader.dto.source.HbaseSourceDTO;
import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.NamespaceNotFoundException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * hbase 新客户端，提供hbase特有的一些方法
 *
 * @author ：wangchuan
 * date：Created in 10:23 上午 2020/12/2
 * company: www.dtstack.com
 */
@Slf4j
public class HbaseClientSpecial implements IHbase {

    // 数据预览最大条数
    private static final Integer MAX_PREVIEW_NUM = 5000;

    // 数据预览默认条数
    private static final Integer DEFAULT_PREVIEW_NUM = 100;

    // rowkey
    private static final String ROWKEY = "rowkey";

    // 列族:列名
    private static final String FAMILY_QUALIFIER = "%s:%s";

    // 列的时间戳
    private static final String TIMESTAMP = "timestamp";

    @Override
    public Boolean isDbExists(ISourceDTO source, String namespace) {
        HbaseSourceDTO hbaseSourceDTO = (HbaseSourceDTO) source;
        Connection connection = null;
        Admin admin = null;
        try {
            //获取hbase连接
            connection = HbaseConnFactory.getHbaseConn(hbaseSourceDTO);
            admin = connection.getAdmin();
            NamespaceDescriptor namespaceDescriptor = admin.getNamespaceDescriptor(namespace);
            if (Objects.nonNull(namespaceDescriptor)) {
                return true;
            }
        } catch (NamespaceNotFoundException namespaceNotFoundException) {
            log.error("namespace [{}] not found!", namespace);
        } catch (Exception e) {
            throw new DtLoaderException(String.format("get namespace exception, namespace：'%s', %s", namespace, e.getMessage()), e);
        } finally {
            close(admin);
            closeConnection(connection, hbaseSourceDTO);
        }
        return false;
    }

    @Override
    public Boolean createHbaseTable(ISourceDTO source, String tbName, String[] colFamily) {
        return createHbaseTable(source, null, tbName, colFamily);
    }

    @Override
    public Boolean createHbaseTable(ISourceDTO source, String namespace, String tbName, String[] colFamily) {
        if (StringUtils.isNotBlank(namespace) && !tbName.contains(":")) {
            tbName = String.format("%s:%s", namespace, tbName);
        }
        HbaseSourceDTO hbaseSourceDTO = (HbaseSourceDTO) source;
        TableName tableName = TableName.valueOf(tbName);
        Connection connection = null;
        Admin admin = null;
        try {
            //获取hbase连接
            connection = HbaseConnFactory.getHbaseConn(hbaseSourceDTO);
            admin = connection.getAdmin();
            if (admin.tableExists(tableName)) {
                throw new DtLoaderException(String.format("The current table already exists！:'%s'", tbName));
            } else {
                HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
                for (String str : colFamily) {
                    HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(str);
                    hTableDescriptor.addFamily(hColumnDescriptor);
                }
                admin.createTable(hTableDescriptor);
                log.info("hbase create table '{}' success!", tableName);
            }
        } catch (DtLoaderException e) {
            throw e;
        } catch (Exception e) {
            throw new DtLoaderException(String.format("hbase failed to create table！namespace：'%s'，table name：'%s'，column family：'%s'", namespace, tbName, Arrays.toString(colFamily)), e);
        } finally {
            close(admin);
            closeConnection(connection, hbaseSourceDTO);
        }
        return true;
    }

    @Override
    public List<String> scanByRegex(ISourceDTO source, String tbName, String regex) {
        HbaseSourceDTO hbaseSourceDTO = (HbaseSourceDTO) source;
        Connection connection = null;
        Table table = null;
        ResultScanner rs = null;
        List<String> results = Lists.newArrayList();
        try {
            //获取hbase连接
            connection = HbaseConnFactory.getHbaseConn(hbaseSourceDTO);
            table = connection.getTable(TableName.valueOf(tbName));
            Scan scan = new Scan();
            org.apache.hadoop.hbase.filter.Filter rowFilter = new org.apache.hadoop.hbase.filter.RowFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator(regex));
            scan.setFilter(rowFilter);
            rs = table.getScanner(scan);
            for (Result r : rs) {
                results.add(Bytes.toString(r.getRow()));
            }
        } catch (DtLoaderException e) {
            throw e;
        } catch (Exception e) {
            throw new DtLoaderException(String.format("Hbase scans data abnormally according to regular！,regex：%s", regex), e);
        } finally {
            close(rs, table);
            closeConnection(connection, hbaseSourceDTO);
        }
        return results;
    }

    @Override
    public Boolean deleteByRowKey(ISourceDTO source, String tbName, String family, String qualifier, List<String> rowKeys) {
        HbaseSourceDTO hbaseSourceDTO = (HbaseSourceDTO) source;
        Connection connection = null;
        Table table = null;
        if (CollectionUtils.isEmpty(rowKeys)) {
            throw new DtLoaderException("The rowKey to be deleted cannot be empty！");
        }
        try {
            //获取hbase连接
            connection = HbaseConnFactory.getHbaseConn(hbaseSourceDTO);
            table = connection.getTable(TableName.valueOf(tbName));
            for (String rowKey : rowKeys) {
                Delete delete = new Delete(Bytes.toBytes(rowKey));
                delete.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));
                table.delete(delete);
            }
            return true;
        } catch (DtLoaderException e) {
            throw e;
        } catch (Exception e) {
            throw new DtLoaderException(String.format("hbase delete data exception! rowKeys： %s,%s", rowKeys, e.getMessage()), e);
        } finally {
            close(table);
            closeConnection(connection, hbaseSourceDTO);
        }
    }

    @Override
    public Boolean putRow(ISourceDTO source, String tableName, String rowKey, String family, String qualifier, String data) {
        HbaseSourceDTO hbaseSourceDTO = (HbaseSourceDTO) source;
        Connection connection = null;
        Table table = null;
        try {
            //获取hbase连接
            connection = HbaseConnFactory.getHbaseConn(hbaseSourceDTO);
            table = connection.getTable(TableName.valueOf(tableName));
            Put put = new Put(Bytes.toBytes(rowKey));
            put.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(data));
            table.put(put);
            return true;
        } catch (DtLoaderException e) {
            throw e;
        } catch (Exception e) {
            throw new DtLoaderException(String.format("hbase insert data exception! rowKey： %s， data： %s, error: %s", rowKey, data, e.getMessage()), e);
        } finally {
            close(table);
            closeConnection(connection, hbaseSourceDTO);
        }
    }

    @Override
    public String getRow(ISourceDTO source, String tableName, String rowKey, String family, String qualifier) {
        HbaseSourceDTO hbaseSourceDTO = (HbaseSourceDTO) source;
        Connection connection = null;
        Table table = null;
        String row = null;
        try {
            //获取hbase连接
            connection = HbaseConnFactory.getHbaseConn(hbaseSourceDTO);
            table = connection.getTable(TableName.valueOf(tableName));
            Get get = new Get(Bytes.toBytes(rowKey));
            get.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));
            Result result = table.get(get);
            row =  Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes(qualifier)));
        } catch (DtLoaderException e) {
            throw e;
        } catch (Exception e) {
            throw new DtLoaderException(String.format("Hbase gets data exception! rowKey： %s , %s", rowKey, e.getMessage()), e);
        } finally {
            close(table);
            closeConnection(connection, hbaseSourceDTO);
        }
        return row;
    }

    @Override
    public List<List<String>> preview(ISourceDTO source, String tableName, Integer previewNum) {
        return preview(source, tableName, Maps.newHashMap(), previewNum);
    }

    @Override
    public List<List<String>> preview(ISourceDTO source, String tableName, List<String> familyList, Integer previewNum) {
        Map<String, List<String>> familyQualifierMap = Maps.newHashMap();
        if (CollectionUtils.isNotEmpty(familyList)) {
            familyList.forEach(family -> familyQualifierMap.put(family, null));
        }
        return preview(source, tableName, familyQualifierMap, previewNum);
    }

    @Override
    public List<List<String>> preview(ISourceDTO source, String tableName, Map<String, List<String>> familyQualifierMap, Integer previewNum) {
        HbaseSourceDTO hbaseSourceDTO = (HbaseSourceDTO) source;
        Connection connection = null;
        Table table = null;
        ResultScanner rs = null;
        List<List<String>> previewList = Lists.newArrayList();
        try {
            // 获取hbase连接
            connection = HbaseConnFactory.getHbaseConn(hbaseSourceDTO);
            table = connection.getTable(TableName.valueOf(tableName));
            Scan scan = new Scan();
            // 计算数据预览条数，最大 5000，默认 100
            if (Objects.isNull(previewNum) || previewNum <= 0) {
                previewNum = DEFAULT_PREVIEW_NUM;
            } else if (previewNum > MAX_PREVIEW_NUM) {
                previewNum = MAX_PREVIEW_NUM;
            }
            // 支持添加列族、列名过滤
            if (MapUtils.isNotEmpty(familyQualifierMap)) {
                for (String family : familyQualifierMap.keySet()) {
                    List<String> qualifiers = familyQualifierMap.get(family);
                    if (CollectionUtils.isNotEmpty(qualifiers)) {
                        for (String qualifier : qualifiers) {
                            scan.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));
                        }
                    } else {
                        scan.addFamily(Bytes.toBytes(family));
                    }
                }
            }
            // 数据预览限制返回条数
            scan.setMaxResultSize(previewNum);
            scan.setFilter(new PageFilter(previewNum));
            rs = table.getScanner(scan);
            List<Result> results = Lists.newArrayList();
            for (Result row : rs) {
                if (CollectionUtils.isEmpty(row.listCells())) {
                    continue;
                }
                results.add(row);
                if (results.size() >= previewNum) {
                    break;
                }
            }
            if (CollectionUtils.isEmpty(results)) {
                return previewList;
            }
            // 列名、值对应信息
            Map<String, List<String>> columnValueMap = Maps.newHashMap();
            // rowKey 集合
            List<String> rowKeyList = Lists.newArrayList();
            // timestamp 集合，取当前rowkey 最新字段更新的时间作为整个rowkey的timestamp
            List<String> timeStampList = Lists.newArrayList();
            columnValueMap.put(ROWKEY, rowKeyList);
            columnValueMap.put(TIMESTAMP, timeStampList);
            // 行号
            int rowNum = 0;
            for (Result result : results) {
                rowNum++;
                // 不设置获取版本，cells中默认只会返回最新版本的cell，不会存在多个版本并存的情况
                List<Cell> cells = result.listCells();
                long timestamp = 0L;
                String rowKey = null;
                for (Cell cell : cells) {
                    rowKey = Bytes.toString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());
                    String family = Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength());
                    String qualifier = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
                    String column = String.format(FAMILY_QUALIFIER, family, qualifier);
                    List<String> columnList = columnValueMap.get(column);
                    // 如果为空则补全 null
                    if (Objects.isNull(columnList)) {
                        columnList = Lists.newArrayList();
                        for (int i = 0; i < rowNum - 1; i++) {
                            columnList.add(null);
                        }
                        columnValueMap.put(column, columnList);
                    }
                    String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                    columnList.add(value);
                    //取到最新变动的时间
                    if (cell.getTimestamp() > timestamp) {
                        timestamp = cell.getTimestamp();
                    }
                }
                List<String> rowKeyColumnList = columnValueMap.get(ROWKEY);
                List<String> timestampColumnList = columnValueMap.get(TIMESTAMP);
                rowKeyColumnList.add(rowKey);
                timestampColumnList.add(String.valueOf(timestamp));
                int finalRowNum = rowNum;
                // 没有的字段补充null值
                columnValueMap.forEach((key, value) -> {
                    if (value.size() < finalRowNum) {
                        value.add(null);
                    }
                });
            }
            List<String> columnMetaDatas = new ArrayList<>(columnValueMap.keySet());
            columnMetaDatas.remove(ROWKEY);
            columnMetaDatas.remove(TIMESTAMP);
            // 排序，将rowKey放第一行，timestamp放最后一行
            Collections.sort(columnMetaDatas);
            columnMetaDatas.add(0, ROWKEY);
            columnMetaDatas.add(TIMESTAMP);
            previewList.add(columnMetaDatas);
            for (int i = 0; i < rowNum; i++) {
                List<String> row = Lists.newArrayList();
                for (String columnMetaData : columnMetaDatas) {
                    row.add(columnValueMap.get(columnMetaData).get(i));
                }
                previewList.add(row);
            }
            return previewList;
        } catch (Exception e) {
            throw new DtLoaderException(String.format("Data preview failed,%s", e.getMessage()), e);
        } finally {
            close(table, rs);
            closeConnection(connection, hbaseSourceDTO);
        }
    }

    /**
     * 关闭hbase连接：当connection不为null且没有开启连接池或者开启kerberos的情况下进行关闭hbase连接
     * @param connection hbase连接
     * @param hbaseSourceDTO hbase数据源信息
     */
    private static void closeConnection(Connection connection, HbaseSourceDTO hbaseSourceDTO) {
        if (connection != null && ((hbaseSourceDTO.getPoolConfig() == null || MapUtils.isNotEmpty(hbaseSourceDTO.getKerberosConfig())))) {
            try {
                connection.close();
            } catch (IOException e) {
                log.error("hbase Close connection exception", e);
            }
        }
    }

    /**
     * 关闭admin、table、resultScanner......
     * @param closeables 可关闭连接、结果集
     */
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
