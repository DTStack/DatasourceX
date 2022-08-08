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

package com.dtstack.dtcenter.loader.cache.client.table;

import com.dtstack.dtcenter.loader.callback.ClassLoaderCallBackMethod;
import com.dtstack.dtcenter.loader.client.ITable;
import com.dtstack.dtcenter.loader.dto.UpsertColumnMetaDTO;
import com.dtstack.dtcenter.loader.dto.canal.BinlogCallBackLogDTO;
import com.dtstack.dtcenter.loader.dto.canal.BinlogConfigDTO;
import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;

import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/**
 * 表相关操作客户端代理类
 *
 * @author ：wangchuan
 * date：Created in 2:17 下午 2020/11/12
 * company: www.dtstack.com
 */
public class TableClientProxy implements ITable {

    ITable targetClient;

    public TableClientProxy(ITable table) {
        this.targetClient = table;
    }

    @Override
    public List<Map<String, Object>> executeQuery(ISourceDTO source, String sql) {
        return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.executeQuery(source, sql),
                targetClient.getClass().getClassLoader());
    }

    @Override
    public Boolean executeSqlWithoutResultSet(ISourceDTO source, String sql) {
        return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.executeSqlWithoutResultSet(source, sql),
                targetClient.getClass().getClassLoader());
    }

    @Override
    public List<String> showPartitions(ISourceDTO source, String tableName) {
        return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.showPartitions(source, tableName),
                targetClient.getClass().getClassLoader());
    }

    @Override
    public Boolean dropTable(ISourceDTO source, String tableName) {
        return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.dropTable(source, tableName),
                targetClient.getClass().getClassLoader());
    }

    @Override
    public Boolean renameTable(ISourceDTO source, String oldTableName, String newTableName) {
        return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.renameTable(source, oldTableName, newTableName),
                targetClient.getClass().getClassLoader());
    }

    @Override
    public Boolean alterTableParams(ISourceDTO source, String tableName, Map<String, String> params) {
        return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.alterTableParams(source, tableName, params),
                targetClient.getClass().getClassLoader());
    }

    @Override
    public Long getTableSize(ISourceDTO source, String schema, String tableName) {
        return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.getTableSize(source, schema, tableName),
                targetClient.getClass().getClassLoader());
    }

    @Override
    public Boolean isView(ISourceDTO source, String schema, String tableName) {
        return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.isView(source, schema, tableName),
                targetClient.getClass().getClassLoader());
    }

    @Override
    public Boolean upsertTableColumn(ISourceDTO source, UpsertColumnMetaDTO columnMetaDTO) {
        return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.upsertTableColumn(source, columnMetaDTO),
                targetClient.getClass().getClassLoader());
    }

    @Override
    public Boolean monitorBinlog(ISourceDTO source, BinlogConfigDTO binlogConfigDTO, Consumer<BinlogCallBackLogDTO> consumer) {
        return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.monitorBinlog(source, binlogConfigDTO, consumer),
                targetClient.getClass().getClassLoader());
    }
}
