package com.dtstack.dtcenter.loader.client.sql;

import com.dtstack.dtcenter.common.exception.DtCenterDefException;
import com.dtstack.dtcenter.loader.ClassLoaderCallBackMethod;
import com.dtstack.dtcenter.loader.client.IClient;
import com.dtstack.dtcenter.loader.downloader.DownloaderProxy;
import com.dtstack.dtcenter.loader.IDownloader;
import com.dtstack.dtcenter.loader.dto.ColumnMetaDTO;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.util.List;
import java.util.Map;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 16:19 2020/1/6
 * @Description 代理实现
 */
@Slf4j
public class DataSourceClientProxy<T> implements IClient<T> {
    private IClient targetClient;

    public DataSourceClientProxy(IClient targetClient) {
        this.targetClient = targetClient;
    }

    @Override
    public Connection getCon(ISourceDTO source) throws Exception {
        try {
            return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.getCon(source),
                    targetClient.getClass().getClassLoader());
        } catch (Exception e) {
            throw new DtCenterDefException(e.getMessage(), e);
        }
    }

    @Override
    public Boolean testCon(ISourceDTO source) {
        try {
            return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.testCon(source),
                    targetClient.getClass().getClassLoader());
        } catch (Exception e) {
            throw new DtCenterDefException(e.getMessage(), e);
        }
    }

    @Override
    public List<Map<String, Object>> executeQuery(ISourceDTO source, SqlQueryDTO queryDTO) throws Exception {
        try {
            return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.executeQuery(source, queryDTO),
                    targetClient.getClass().getClassLoader());
        } catch (Exception e) {
            throw new DtCenterDefException(e.getMessage(), e);
        }
    }

    @Override
    public Boolean executeSqlWithoutResultSet(ISourceDTO source, SqlQueryDTO queryDTO) throws Exception {
        try {
            return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.executeSqlWithoutResultSet(source,
                    queryDTO), targetClient.getClass().getClassLoader());
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            return false;
        }
    }

    @Override
    public List<String> getTableList(ISourceDTO source, SqlQueryDTO queryDTO) throws Exception {
        try {
            return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.getTableList(source, queryDTO),
                    targetClient.getClass().getClassLoader());
        } catch (Exception e) {
            throw new DtCenterDefException(e.getMessage(), e);
        }
    }

    @Override
    public List<String> getColumnClassInfo(ISourceDTO source, SqlQueryDTO queryDTO) throws Exception {
        try {
            return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.getColumnClassInfo(source, queryDTO),
                    targetClient.getClass().getClassLoader());
        } catch (Exception e) {
            throw new DtCenterDefException(e.getMessage(), e);
        }
    }

    @Override
    public List<ColumnMetaDTO> getColumnMetaData(ISourceDTO source, SqlQueryDTO queryDTO) throws Exception {
        try {
            return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.getColumnMetaData(source, queryDTO),
                    targetClient.getClass().getClassLoader());
        } catch (Exception e) {
            throw new DtCenterDefException(e.getMessage(), e);
        }
    }

    @Override
    public List<ColumnMetaDTO> getColumnMetaDataWithSql(ISourceDTO source, SqlQueryDTO queryDTO) throws Exception {
        try {
            return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.getColumnMetaDataWithSql(source, queryDTO),
                    targetClient.getClass().getClassLoader());
        } catch (Exception e) {
            throw new DtCenterDefException(e.getMessage(), e);
        }
    }

    @Override
    public List<ColumnMetaDTO> getFlinkColumnMetaData(ISourceDTO source, SqlQueryDTO queryDTO) throws Exception {
        try {
            return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.getFlinkColumnMetaData(source, queryDTO),
                    targetClient.getClass().getClassLoader());
        } catch (Exception e) {
            throw new DtCenterDefException(e.getMessage(), e);
        }
    }

    @Override
    public String getTableMetaComment(ISourceDTO source, SqlQueryDTO queryDTO) throws Exception {
        try {
            return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.getTableMetaComment(source, queryDTO),
                    targetClient.getClass().getClassLoader());
        } catch (Exception e) {
            throw new DtCenterDefException(e.getMessage(), e);
        }
    }

    @Override
    public List<List<Object>> getPreview(ISourceDTO source, SqlQueryDTO queryDTO) {
        try {
            return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.getPreview(source, queryDTO),
                    targetClient.getClass().getClassLoader());
        } catch (Exception e) {
            throw new DtCenterDefException(e.getMessage(), e);
        }
    }

    @Override
    public IDownloader getDownloader(ISourceDTO source, SqlQueryDTO queryDTO) {
        try {
            return ClassLoaderCallBackMethod.callbackAndReset(() -> new DownloaderProxy(targetClient.getDownloader(source, queryDTO)),
                    targetClient.getClass().getClassLoader());
        } catch (Exception e) {
            throw new DtCenterDefException(e.getMessage(), e);
        }
    }

    @Override
    public List<String> getAllDatabases(ISourceDTO source, SqlQueryDTO queryDTO) throws Exception {
        try {
            return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.getAllDatabases(source, queryDTO),
                    targetClient.getClass().getClassLoader());
        } catch (Exception e) {
            throw new DtCenterDefException(e.getMessage(), e);
        }
    }

    @Override
    public String getCreateTableSql(ISourceDTO source, SqlQueryDTO queryDTO) throws Exception {
        try {
            return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.getCreateTableSql(source, queryDTO),
                    targetClient.getClass().getClassLoader());
        } catch (Exception e) {
            throw new DtCenterDefException(e.getMessage(), e);
        }
    }

    @Override
    public List<ColumnMetaDTO> getPartitionColumn(ISourceDTO source, SqlQueryDTO queryDTO) throws Exception {
        try {
            return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.getPartitionColumn(source, queryDTO),
                    targetClient.getClass().getClassLoader());
        } catch (Exception e) {
            throw new DtCenterDefException(e.getMessage(), e);
        }
    }
}
