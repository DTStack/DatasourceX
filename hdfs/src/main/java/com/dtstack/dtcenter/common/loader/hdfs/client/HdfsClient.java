package com.dtstack.dtcenter.common.loader.hdfs.client;

import com.dtstack.dtcenter.common.loader.hdfs.HdfsConnFactory;
import com.dtstack.dtcenter.loader.IDownloader;
import com.dtstack.dtcenter.loader.client.IClient;
import com.dtstack.dtcenter.loader.dto.ColumnMetaDTO;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.Table;
import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;

import java.sql.Connection;
import java.util.List;
import java.util.Map;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 16:53 2020/2/27
 * @Description：hdfs 客户端
 */
public class HdfsClient<T> implements IClient<T> {
    private HdfsConnFactory hdfsConnFactory = new HdfsConnFactory();

    @Override
    public Boolean testCon(ISourceDTO source) {
        return hdfsConnFactory.testConn(source);
    }

    /************************************** 不支持的方法 ****************************************/
    @Override
    public Connection getCon(ISourceDTO iSourceDTO) {
        throw new DtLoaderException("Not Support");
    }

    @Override
    public List<Map<String, Object>> executeQuery(ISourceDTO iSourceDTO, SqlQueryDTO queryDTO) {
        throw new DtLoaderException("Not Support");
    }

    @Override
    public Boolean executeSqlWithoutResultSet(ISourceDTO iSourceDTO, SqlQueryDTO queryDTO) {
        throw new DtLoaderException("Not Support");
    }

    @Override
    public List<String> getTableList(ISourceDTO iSourceDTO, SqlQueryDTO queryDTO) {
        throw new DtLoaderException("Not Support");
    }

    @Override
    public List<String> getTableListBySchema(ISourceDTO source, SqlQueryDTO queryDTO) {
        throw new DtLoaderException("Not Support");
    }

    @Override
    public List<String> getColumnClassInfo(ISourceDTO iSourceDTO, SqlQueryDTO queryDTO) {
        throw new DtLoaderException("Not Support");
    }

    @Override
    public List<ColumnMetaDTO> getColumnMetaData(ISourceDTO iSourceDTO, SqlQueryDTO queryDTO) {
        throw new DtLoaderException("Not Support");
    }

    @Override
    public List<ColumnMetaDTO> getColumnMetaDataWithSql(ISourceDTO iSource, SqlQueryDTO queryDTO) {
        throw new DtLoaderException("Not Support");
    }

    @Override
    public List<ColumnMetaDTO> getFlinkColumnMetaData(ISourceDTO source, SqlQueryDTO queryDTO) {
        throw new DtLoaderException("Not Support");
    }

    @Override
    public String getTableMetaComment(ISourceDTO source, SqlQueryDTO queryDTO) {
        throw new DtLoaderException("Not Support");
    }

    @Override
    public List<List<Object>> getPreview(ISourceDTO iSource, SqlQueryDTO queryDTO) {
        throw new DtLoaderException("Not Support");
    }

    @Override
    public IDownloader getDownloader(ISourceDTO source, SqlQueryDTO queryDTO) {
        throw new DtLoaderException("Not Support");
    }

    @Override
    public List<String> getAllDatabases(ISourceDTO source, SqlQueryDTO queryDTO) {
        throw new DtLoaderException("Not Support");
    }

    @Override
    public String getCreateTableSql(ISourceDTO source, SqlQueryDTO queryDTO) {
        throw new DtLoaderException("Not Support");
    }

    @Override
    public List<ColumnMetaDTO> getPartitionColumn(ISourceDTO source, SqlQueryDTO queryDTO) {
        throw new DtLoaderException("Not Support");
    }

    @Override
    public Table getTable(ISourceDTO source, SqlQueryDTO queryDTO) {
        throw new DtLoaderException("Not Support");
    }

    @Override
    public String getCurrentDatabase(ISourceDTO source) {
        throw new DtLoaderException("Not Support");
    }

    @Override
    public Boolean createDatabase(ISourceDTO source, String dbName, String comment) {
        throw new DtLoaderException("hdfs数据源不支持该方法");
    }

    @Override
    public Boolean isDatabaseExists(ISourceDTO source, String dbName) {
        throw new DtLoaderException("hdfs数据源不支持该方法");
    }

    @Override
    public Boolean isTableExistsInDatabase(ISourceDTO source, String tableName, String dbName) {
        throw new DtLoaderException("hdfs数据源不支持该方法");
    }
}