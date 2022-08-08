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

package com.dtstack.dtcenter.loader.cache.client.sql;

import com.dtstack.dtcenter.loader.client.IClient;
import com.dtstack.dtcenter.loader.downloader.DownloaderProxy;
import com.dtstack.dtcenter.loader.dto.ColumnMetaDTO;
import com.dtstack.dtcenter.loader.dto.Database;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.Table;
import com.dtstack.dtcenter.loader.dto.WriteFileDTO;
import com.dtstack.dtcenter.loader.dto.TableInfo;
import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;
import com.dtstack.dtcenter.loader.metadata.MetaDataCollectManager;
import com.dtstack.rpc.download.IDownloader;
import com.dtstack.dtcenter.loader.callback.ClassLoaderCallBackMethod;
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
public class DataSourceClientProxy implements IClient {
    private IClient targetClient;

    public DataSourceClientProxy(IClient targetClient) {
        this.targetClient = targetClient;
    }

    @Override
    public Boolean testCon(ISourceDTO source) {
        return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.testCon(source),
                targetClient.getClass().getClassLoader());
    }

    @Override
    public List<Map<String, Object>> executeQuery(ISourceDTO source, SqlQueryDTO queryDTO) {
        return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.executeQuery(source, queryDTO),
                targetClient.getClass().getClassLoader());
    }

    @Override
    public Map<String, List<Map<String, Object>>> executeMultiQuery(ISourceDTO source, SqlQueryDTO queryDTO) {
        return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.executeMultiQuery(source, queryDTO),
                targetClient.getClass().getClassLoader());
    }

    @Override
    public Boolean executeBatchQuery(ISourceDTO source, SqlQueryDTO queryDTO) {
        return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.executeBatchQuery(source, queryDTO),
                targetClient.getClass().getClassLoader());
    }

    @Override
    public Integer executeUpdate(ISourceDTO source, SqlQueryDTO queryDTO) {
        return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.executeUpdate(source, queryDTO),
                targetClient.getClass().getClassLoader());
    }

    @Override
    public Boolean executeSqlWithoutResultSet(ISourceDTO source, SqlQueryDTO queryDTO) {
        return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.executeSqlWithoutResultSet(source,
                queryDTO), targetClient.getClass().getClassLoader());
    }

    @Override
    public List<String> getTableList(ISourceDTO source, SqlQueryDTO queryDTO) {
        return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.getTableList(source, queryDTO),
                targetClient.getClass().getClassLoader());
    }

    @Override
    public List<String> getTableListBySchema(ISourceDTO source, SqlQueryDTO queryDTO) {
        return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.getTableListBySchema(source, queryDTO),
                targetClient.getClass().getClassLoader());
    }

    @Override
    public List<String> getColumnClassInfo(ISourceDTO source, SqlQueryDTO queryDTO) {
        return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.getColumnClassInfo(source, queryDTO),
                targetClient.getClass().getClassLoader());
    }

    @Override
    public List<ColumnMetaDTO> getColumnMetaData(ISourceDTO source, SqlQueryDTO queryDTO) {
        return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.getColumnMetaData(source, queryDTO),
                targetClient.getClass().getClassLoader());
    }

    @Override
    public List<ColumnMetaDTO> getColumnMetaDataWithSql(ISourceDTO source, SqlQueryDTO queryDTO) {
        return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.getColumnMetaDataWithSql(source, queryDTO),
                targetClient.getClass().getClassLoader());
    }

    @Override
    public List<ColumnMetaDTO> getFlinkColumnMetaData(ISourceDTO source, SqlQueryDTO queryDTO) {
        return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.getFlinkColumnMetaData(source, queryDTO),
                targetClient.getClass().getClassLoader());
    }

    @Override
    public String getTableMetaComment(ISourceDTO source, SqlQueryDTO queryDTO) {
        return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.getTableMetaComment(source, queryDTO),
                targetClient.getClass().getClassLoader());
    }

    @Override
    public List<List<Object>> getPreview(ISourceDTO source, SqlQueryDTO queryDTO) {
        return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.getPreview(source, queryDTO),
                targetClient.getClass().getClassLoader());
    }

    @Override
    public IDownloader getDownloader(ISourceDTO source, SqlQueryDTO queryDTO) {
        return ClassLoaderCallBackMethod.callbackAndReset(() -> new DownloaderProxy(targetClient.getDownloader(source, queryDTO)),
                targetClient.getClass().getClassLoader());
    }

    @Override
    public IDownloader getDownloader(ISourceDTO source, String sql, Integer pageSize) throws Exception {
        return ClassLoaderCallBackMethod.callbackAndReset(() -> new DownloaderProxy(targetClient.getDownloader(source, sql, pageSize)),
                targetClient.getClass().getClassLoader());
    }

    @Override
    public List<String> getAllDatabases(ISourceDTO source, SqlQueryDTO queryDTO) {
        return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.getAllDatabases(source, queryDTO),
                targetClient.getClass().getClassLoader());
    }

    @Override
    public List<String> getRootDatabases(ISourceDTO source, SqlQueryDTO queryDTO) {
        return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.getRootDatabases(source, queryDTO),
                targetClient.getClass().getClassLoader());
    }

    @Override
    public String getCreateTableSql(ISourceDTO source, SqlQueryDTO queryDTO) {
        return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.getCreateTableSql(source, queryDTO),
                targetClient.getClass().getClassLoader());
    }

    @Override
    public List<ColumnMetaDTO> getPartitionColumn(ISourceDTO source, SqlQueryDTO queryDTO) {
        return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.getPartitionColumn(source, queryDTO),
                targetClient.getClass().getClassLoader());
    }

    @Override
    public Table getTable(ISourceDTO source, SqlQueryDTO queryDTO) {
        return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.getTable(source, queryDTO),
                targetClient.getClass().getClassLoader());
    }

    @Override
    public String getCurrentDatabase(ISourceDTO source) {
        return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.getCurrentDatabase(source),
                targetClient.getClass().getClassLoader());
    }

    @Override
    public Boolean createDatabase(ISourceDTO source, String dbName, String comment) {
        return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.createDatabase(source, dbName, comment),
                targetClient.getClass().getClassLoader());
    }

    @Override
    public Boolean isDatabaseExists(ISourceDTO source, String dbName) {
        return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.isDatabaseExists(source, dbName),
                targetClient.getClass().getClassLoader());
    }

    @Override
    public Boolean isTableExistsInDatabase(ISourceDTO source, String tableName, String dbName) {
        return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.isTableExistsInDatabase(source, tableName, dbName),
                targetClient.getClass().getClassLoader());
    }

    @Override
    public List<String> getCatalogs(ISourceDTO source) {
        return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.getCatalogs(source),
                targetClient.getClass().getClassLoader());
    }

    @Override
    public String getVersion(ISourceDTO source) {
        return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.getVersion(source),
                targetClient.getClass().getClassLoader());
    }

    @Override
    public List<String> listFileNames(ISourceDTO sourceDTO, String path, Boolean includeDir, Boolean recursive, Integer maxNum, String regexStr) {
        return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.listFileNames(sourceDTO, path, includeDir, recursive, maxNum, regexStr),
                targetClient.getClass().getClassLoader());
    }

    @Override
    public Database getDatabase(ISourceDTO sourceDTO, String dbName) {
        return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.getDatabase(sourceDTO, dbName),
                targetClient.getClass().getClassLoader());
    }

    @Override
    public Boolean writeByFile(ISourceDTO sourceDTO, WriteFileDTO writeFileDTO) {
        return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.writeByFile(sourceDTO, writeFileDTO),
                targetClient.getClass().getClassLoader());
    }

    @Override
    public TableInfo getTableInfo(ISourceDTO sourceDTO, String tableName) {
        return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.getTableInfo(sourceDTO, tableName),
                targetClient.getClass().getClassLoader());
    }

    @Override
    public MetaDataCollectManager getMetadataCollectManager(ISourceDTO sourceDTO) {
        return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.getMetadataCollectManager(sourceDTO),
                targetClient.getClass().getClassLoader());
    }
}
