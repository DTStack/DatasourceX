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

package com.dtstack.dtcenter.loader.client.hbase;

import com.dtstack.dtcenter.loader.ClassLoaderCallBackMethod;
import com.dtstack.dtcenter.loader.client.IHbase;
import com.dtstack.dtcenter.loader.dto.HbaseQueryDTO;
import com.dtstack.dtcenter.loader.dto.filter.TimestampFilter;
import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;

import java.util.List;
import java.util.Map;

/**
 * hbase客户端代理
 *
 * @author ：wangchuan
 * date：Created in 9:38 上午 2020/12/2
 * company: www.dtstack.com
 */
public class HbaseProxy implements IHbase {
    IHbase targetClient = null;

    public HbaseProxy(IHbase hbase) {
        this.targetClient = hbase;
    }

    @Override
    public Boolean isDbExists(ISourceDTO source, String namespace) {
        return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.isDbExists(source, namespace),
                targetClient.getClass().getClassLoader());
    }

    @Override
    public Boolean createHbaseTable(ISourceDTO source, String tableName, String[] colFamily) {
        return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.createHbaseTable(source, tableName, colFamily),
                targetClient.getClass().getClassLoader());
    }

    @Override
    public Boolean createHbaseTable(ISourceDTO source, String namespace, String tableName, String[] colFamily) {
        return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.createHbaseTable(source, namespace, tableName, colFamily),
                targetClient.getClass().getClassLoader());
    }

    @Override
    public Boolean deleteHbaseTable(ISourceDTO source, String tableName) {
        return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.deleteHbaseTable(source, tableName),
                targetClient.getClass().getClassLoader());
    }

    @Override
    public Boolean deleteHbaseTable(ISourceDTO source, String namespace, String tableName) {
        return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.deleteHbaseTable(source, namespace, tableName),
                targetClient.getClass().getClassLoader());
    }

    @Override
    public List<String> scanByRegex(ISourceDTO source, String tableName, String regex) {
        return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.scanByRegex(source, tableName, regex),
                targetClient.getClass().getClassLoader());
    }

    @Override
    public Boolean deleteByRowKey(ISourceDTO source, String tableName, String family, String qualifier, List<String> rowKeys) {
        return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.deleteByRowKey(source, tableName, family, qualifier, rowKeys),
                targetClient.getClass().getClassLoader());
    }

    @Override
    public Boolean putRow(ISourceDTO source, String tableName, String rowKey, String family, String qualifier, String data) {
        return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.putRow(source, tableName, rowKey, family, qualifier, data),
                targetClient.getClass().getClassLoader());
    }

    @Override
    public String getRow(ISourceDTO source, String tableName, String rowKey, String family, String qualifier) {
        return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.getRow(source, tableName, rowKey, family, qualifier),
                targetClient.getClass().getClassLoader());
    }

    @Override
    public List<List<String>> preview(ISourceDTO source, String tableName, Integer previewNum) {
        return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.preview(source, tableName, previewNum),
                targetClient.getClass().getClassLoader());
    }

    @Override
    public List<List<String>> preview(ISourceDTO source, String tableName, List<String> familyList, Integer previewNum) {
        return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.preview(source, tableName, familyList, previewNum),
                targetClient.getClass().getClassLoader());
    }

    @Override
    public List<List<String>> preview(ISourceDTO source, String tableName, Map<String, List<String>> familyQualifierMap, Integer previewNum) {
        return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.preview(source, tableName, familyQualifierMap, previewNum),
                targetClient.getClass().getClassLoader());
    }

    @Override
    public List<Map<String, Object>> executeQuery(ISourceDTO source, HbaseQueryDTO hbaseQueryDTO, TimestampFilter timestampFilter) {
        return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.executeQuery(source, hbaseQueryDTO, timestampFilter),
                targetClient.getClass().getClassLoader());
    }
}
