package com.dtstack.dtcenter.common.loader.sqlserver;

import com.dtstack.dtcenter.common.exception.DtCenterDefException;
import com.dtstack.dtcenter.loader.IDownloader;
import com.dtstack.sql.Column;
import com.dtstack.sql.utils.SqlFormatUtil;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @company: www.dtstack.com
 * @Author ：wangchuan
 * @Date ：Created in 下午5:52 2020/5/29
 * @Description：
 */

public class SqlServerDownloader implements IDownloader {

    private int pageNum;

    private int pageAll;

    private List<Column> columnNames;

    private int totalLine;

    private Connection connection;

    private String sql;

    private String schema;

    private Statement statement;

    private int pageSize;

    private int columnCount;

    public SqlServerDownloader(Connection connection, String sql, String schema) {
        this.connection = connection;
        this.sql = SqlFormatUtil.formatSql(sql);
        this.schema = schema;
    }

    @Override
    public void configure() throws Exception {
        if (null == connection || StringUtils.isEmpty(sql)) {
            throw new DtCenterDefException("文件不存在");
        }
        totalLine = 0;
        pageSize = 100;
        pageNum = 1;
        statement = connection.createStatement();
        if (StringUtils.isNotEmpty(schema)) {
            //选择schema
            String useSchema = String.format("USE %s", schema);
            statement.execute(useSchema);
        }
        String countSQL = String.format("SELECT COUNT(*) FROM (%s) temp", sql);
        ResultSet resultSet = statement.executeQuery(countSQL);
        while (resultSet.next()) {
            //获取总行数
            totalLine = resultSet.getInt(1);
        }
        //获取列信息
        String showColumns = String.format("SELECT top 1 * FROM (%s) t", sql);
        resultSet = statement.executeQuery(showColumns);
        columnNames = new ArrayList<>();
        columnCount = resultSet.getMetaData().getColumnCount();
        for (int i = 1; i <= columnCount; i++) {
            Column column = new Column();
            column.setName(resultSet.getMetaData().getColumnName(i));
            column.setType(resultSet.getMetaData().getColumnTypeName(i));
            column.setIndex(i);
            columnNames.add(column);
        }
        //获取总页数
        pageAll = (int) Math.ceil(totalLine/(double)pageSize);
        resultSet.close();
    }

    @Override
    public List<String> getMetaInfo() throws Exception {
        if (CollectionUtils.isNotEmpty(columnNames)) {
            return columnNames.stream().map(Column::getName).collect(Collectors.toList());
        }
        return null;
    }

    @Override
    public List<List<String>> readNext() throws Exception {
        //分页查询，一次一百条
        //todo 没找到适合的分页
        String limitSQL = String.format("select top %s * from (%s) as t where t.%s not in (select top %s m.%s from (%s) m) ", pageSize*pageNum - pageSize*(pageNum-1), sql, columnNames.get(0).getName(), pageSize*(pageNum-1), columnNames.get(0).getName(), sql);
        ResultSet resultSet = statement.executeQuery(limitSQL);
        List<List<String>> pageTemp = new ArrayList<>(100);
        while (resultSet.next()) {
            List<String> columns = new ArrayList<>(columnCount);
            for (int i = 1; i <= columnCount; i++) {
                columns.add(resultSet.getString(i));
            }
            pageTemp.add(columns);
        }
        resultSet.close();
        pageNum++;
        return pageTemp;
    }

    @Override
    public boolean reachedEnd() throws Exception {
        return pageAll < pageNum;
    }

    @Override
    public void close() throws Exception {
        statement.close();
        connection.close();
    }

    @Override
    public String getFileName() {
        return null;
    }
}
