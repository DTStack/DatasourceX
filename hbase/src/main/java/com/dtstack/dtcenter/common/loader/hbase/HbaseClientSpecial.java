package com.dtstack.dtcenter.common.loader.hbase;

import com.dtstack.dtcenter.loader.client.IHbase;
import com.dtstack.dtcenter.loader.dto.source.HbaseSourceDTO;
import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
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
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
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
            throw new DtLoaderException(String.format("获取namespace异常, namespace：'%s'", namespace), e);
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
                throw new DtLoaderException(String.format("当前表已经存在！:'%s'", tbName));
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
            throw new DtLoaderException(String.format("hbase创建表失败！namespace：'%s'，表名：'%s'，列族：'%s'", namespace, tbName, Arrays.toString(colFamily)), e);
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
            throw new DtLoaderException(String.format("hbase根据正则扫描数据异常！,regex：%s", regex), e);
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
            log.info("需要删除的rowKey为空！");
            throw new DtLoaderException("需要删除的rowKey不能为空！");
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
            throw new DtLoaderException(String.format("hbase删除数据异常! rowKeys： %s", rowKeys), e);
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
            throw new DtLoaderException(String.format("hbase插入数据异常! rowKey： %s， data： %s", rowKey, data), e);
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
            throw new DtLoaderException(String.format("hbase获取数据异常! rowKey： %s ", rowKey), e);
        } finally {
            close(table);
            closeConnection(connection, hbaseSourceDTO);
        }
        return row;
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
                log.error("hbase 关闭连接异常", e);
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
            log.error("hbase closeable close error", e);
            throw new DtLoaderException("hbase can not close table error", e);
        }
    }
}
