package com.dtstack.dtcenter.common.loader.common.nosql;

import com.dtstack.dtcenter.common.loader.common.exception.ErrorCode;
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
 * 非关系型数据库抽象类，用于屏蔽一些不需要实现的方法
 *
 * @author ：wangchuan
 * date：Created in 下午4:19 2021/3/23
 * company: www.dtstack.com
 */
public abstract class AbsNoSqlClient<T> implements IClient<T> {

    @Override
    public Connection getCon(ISourceDTO source) {
        throw new DtLoaderException(ErrorCode.NOT_SUPPORT.getDesc());
    }

    @Override
    public Connection getCon(ISourceDTO source, String taskParams) {
        throw new DtLoaderException(ErrorCode.NOT_SUPPORT.getDesc());
    }

    @Override
    public List<Map<String, Object>> executeQuery(ISourceDTO source, SqlQueryDTO queryDTO) {
        throw new DtLoaderException(ErrorCode.NOT_SUPPORT.getDesc());
    }

    @Override
    public Boolean executeSqlWithoutResultSet(ISourceDTO source, SqlQueryDTO queryDTO) {
        throw new DtLoaderException(ErrorCode.NOT_SUPPORT.getDesc());
    }

    @Override
    public List<String> getTableListBySchema(ISourceDTO source, SqlQueryDTO queryDTO) {
        throw new DtLoaderException(ErrorCode.NOT_SUPPORT.getDesc());
    }

    @Override
    public List<String> getColumnClassInfo(ISourceDTO source, SqlQueryDTO queryDTO) {
        throw new DtLoaderException(ErrorCode.NOT_SUPPORT.getDesc());
    }

    @Override
    public List<ColumnMetaDTO> getColumnMetaDataWithSql(ISourceDTO source, SqlQueryDTO queryDTO) {
        throw new DtLoaderException(ErrorCode.NOT_SUPPORT.getDesc());
    }

    @Override
    public String getTableMetaComment(ISourceDTO source, SqlQueryDTO queryDTO) {
        throw new DtLoaderException(ErrorCode.NOT_SUPPORT.getDesc());
    }

    @Override
    public IDownloader getDownloader(ISourceDTO source, SqlQueryDTO queryDTO) {
        throw new DtLoaderException(ErrorCode.NOT_SUPPORT.getDesc());
    }

    @Override
    public String getCreateTableSql(ISourceDTO source, SqlQueryDTO queryDTO) {
        throw new DtLoaderException(ErrorCode.NOT_SUPPORT.getDesc());
    }

    @Override
    public List<ColumnMetaDTO> getPartitionColumn(ISourceDTO source, SqlQueryDTO queryDTO) {
        throw new DtLoaderException(ErrorCode.NOT_SUPPORT.getDesc());
    }

    @Override
    public Table getTable(ISourceDTO source, SqlQueryDTO queryDTO) {
        throw new DtLoaderException(ErrorCode.NOT_SUPPORT.getDesc());
    }

    @Override
    public String getCurrentDatabase(ISourceDTO source) {
        throw new DtLoaderException(ErrorCode.NOT_SUPPORT.getDesc());
    }

    @Override
    public Boolean createDatabase(ISourceDTO source, String dbName, String comment) {
        throw new DtLoaderException(ErrorCode.NOT_SUPPORT.getDesc());
    }

    @Override
    public Boolean isDatabaseExists(ISourceDTO source, String dbName) {
        throw new DtLoaderException(ErrorCode.NOT_SUPPORT.getDesc());
    }

    @Override
    public Boolean isTableExistsInDatabase(ISourceDTO source, String tableName, String dbName) {
        throw new DtLoaderException(ErrorCode.NOT_SUPPORT.getDesc());
    }

    @Override
    public List<String> getCatalogs(ISourceDTO source) {
        throw new DtLoaderException(ErrorCode.NOT_SUPPORT.getDesc());
    }

    @Override
    public List<String> getAllDatabases(ISourceDTO source, SqlQueryDTO queryDTO) {
        throw new DtLoaderException(ErrorCode.NOT_SUPPORT.getDesc());
    }

    @Override
    public List<String> getRootDatabases(ISourceDTO source, SqlQueryDTO queryDTO) {
        throw new DtLoaderException(ErrorCode.NOT_SUPPORT.getDesc());
    }

    @Override
    public List<ColumnMetaDTO> getColumnMetaData(ISourceDTO source, SqlQueryDTO queryDTO) {
        throw new DtLoaderException(ErrorCode.NOT_SUPPORT.getDesc());
    }

    @Override
    public List<ColumnMetaDTO> getFlinkColumnMetaData(ISourceDTO source, SqlQueryDTO queryDTO) {
        return getColumnMetaData(source, queryDTO);
    }

    @Override
    public List<String> getTableList(ISourceDTO source, SqlQueryDTO queryDTO) {
        throw new DtLoaderException(ErrorCode.NOT_SUPPORT.getDesc());
    }

    @Override
    public List<List<Object>> getPreview(ISourceDTO source, SqlQueryDTO queryDTO) {
        throw new DtLoaderException(ErrorCode.NOT_SUPPORT.getDesc());
    }
}
