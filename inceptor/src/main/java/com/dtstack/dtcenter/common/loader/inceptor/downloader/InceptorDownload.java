package com.dtstack.dtcenter.common.loader.inceptor.downloader;

import com.dtstack.dtcenter.loader.IDownloader;
import com.dtstack.dtcenter.loader.dto.Column;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.BooleanUtils;
import org.apache.commons.lang.StringUtils;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * inceptor 表数据下载器
 *
 * @author ：wangchuan
 * date：Created in 上午9:53 2021/4/6
 * company: www.dtstack.com
 */
@Slf4j
public class InceptorDownload implements IDownloader {

    /**
     * 执行的 sql
     */
    private final String sql;

    /**
     * 数据库连接
     */
    private final Connection connection;

    /**
     * 连接操作对象
     */
    private Statement statement;

    /**
     * 当前读取页码
     */
    private int pageNum = 1;

    /**
     * 每页数据条数
     */
    private final int pageSize = 100;

    /**
     * 总页数
     */
    private int pageAll;

    /**
     * 总数据条数
     */
    private int totalLine = 0;

    /**
     * 列字段
     */
    private List<Column> columnNames;

    /**
     * 详细的数据信息
     */
    private List<List<String>> pageInfo;

    /**
     * 表字段条数
     */
    private int columnCount;

    /**
     * 是否已经调用 configure 方法
     */
    private boolean isConfigure = false;

    public InceptorDownload(Connection connection, String sql) {
        this.connection = connection;
        this.sql = sql;
    }

    @Override
    public boolean configure() throws Exception {
        if (BooleanUtils.isTrue(isConfigure)) {
            // 避免 configure 方法重复调用
            return true;
        }
        if (null == connection || StringUtils.isEmpty(sql)) {
            throw new DtLoaderException("impala connection acquisition failed or execution SQL is empty");
        }
        statement = connection.createStatement();

        String countSQL = String.format("SELECT COUNT(*) FROM (%s) temp", sql);
        try (ResultSet resultSet = statement.executeQuery(countSQL);) {
            while (resultSet.next()) {
                // 获取总行数
                totalLine = resultSet.getInt(1);
            }
        }
        // 获取列信息
        String showColumns = String.format("SELECT * FROM (%s) t LIMIT 1", sql);
        try (ResultSet resultSet = statement.executeQuery(showColumns)) {
            columnNames = new ArrayList<>();
            while (resultSet.next()) {
                columnCount = resultSet.getMetaData().getColumnCount();
                for (int i = 1; i <= columnCount; i++) {
                    Column column = new Column();
                    column.setName(resultSet.getMetaData().getColumnName(i));
                    column.setType(resultSet.getMetaData().getColumnTypeName(i));
                    column.setIndex(i);
                    columnNames.add(column);
                }
            }
            //获取总页数
            pageAll = (int) Math.ceil(totalLine / (double) pageSize);
        }
        isConfigure = true;
        log.info("impalaDownload: executed SQL:{}, totalLine:{}, columnNames:{}, columnCount{}", sql, totalLine, columnNames, columnCount);
        return true;
    }

    @Override
    public List<String> getMetaInfo() {
        if (CollectionUtils.isNotEmpty(columnNames)) {
            return columnNames.stream().map(Column::getName).collect(Collectors.toList());
        }
        return null;
    }

    @Override
    public List<List<String>> readNext() {
        //分页查询，一次一百条
        String limitSQL = String.format("SELECT * FROM (%s) t limit %s,%s", sql, pageSize * (pageNum - 1), pageSize);
        List<List<String>> pageTemp = new ArrayList<>(100);

        try (ResultSet resultSet = statement.executeQuery(limitSQL)) {
            while (resultSet.next()) {
                List<String> columns = new ArrayList<>(columnCount);
                for (int i = 1; i <= columnCount; i++) {
                    columns.add(resultSet.getString(i));
                }
                pageTemp.add(columns);
            }
        } catch (Exception e) {
            throw new DtLoaderException("read Mysql message exception : " + e.getMessage(), e);
        }

        pageNum++;
        return pageTemp;
    }

    @Override
    public boolean reachedEnd() {
        return pageAll < pageNum;
    }

    @Override
    public boolean close() throws Exception {
        statement.close();
        connection.close();
        return true;
    }

    @Override
    public String getFileName() {
        return null;
    }

    @Override
    public List<String> getContainers() {
        return null;
    }
}
