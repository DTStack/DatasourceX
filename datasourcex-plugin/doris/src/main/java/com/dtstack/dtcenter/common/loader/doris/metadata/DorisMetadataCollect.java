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

package com.dtstack.dtcenter.common.loader.doris.metadata;

import com.alibaba.fastjson.JSON;
import com.dtstack.dtcenter.common.loader.doris.DorisConnFactory;
import com.dtstack.dtcenter.common.loader.doris.metadata.cons.DorisConstants;
import com.dtstack.dtcenter.common.loader.doris.metadata.entity.DorisColumnEntity;
import com.dtstack.dtcenter.common.loader.doris.metadata.entity.DorisColumnMetaEntity;
import com.dtstack.dtcenter.common.loader.doris.metadata.entity.DorisIndexEntity;
import com.dtstack.dtcenter.common.loader.doris.metadata.entity.DorisTableEntity;
import com.dtstack.dtcenter.common.loader.doris.metadata.entity.MetadataDorisEntity;
import com.dtstack.dtcenter.common.loader.doris.metadata.rest.ListResponse;
import com.dtstack.dtcenter.common.loader.doris.metadata.rest.RowCountResponse;
import com.dtstack.dtcenter.common.loader.doris.metadata.rest.SchemaResponse;
import com.dtstack.dtcenter.common.loader.doris.metadata.util.JsonUtils;
import com.dtstack.dtcenter.common.loader.rdbms.ConnFactory;
import com.dtstack.dtcenter.common.loader.rdbms.metadata.RdbmsMetaDataCollect;
import com.dtstack.dtcenter.common.loader.restful.http.HttpClient;
import com.dtstack.dtcenter.common.loader.restful.http.HttpClientFactory;
import com.dtstack.dtcenter.loader.dto.metadata.entity.rdb.MetadataRdbEntity;
import com.dtstack.dtcenter.loader.dto.restful.Response;
import com.dtstack.dtcenter.loader.dto.source.DorisRestfulSourceDTO;
import com.dtstack.dtcenter.loader.dto.source.DorisSourceDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.dtstack.dtcenter.loader.utils.AssertUtils;
import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.dtstack.dtcenter.common.loader.doris.metadata.cons.DorisConstants.DEFAULT_CLUSTER;
import static com.dtstack.dtcenter.common.loader.doris.metadata.cons.DorisConstants.SUCCESS_FLAG;

/**
 * Doris元数据收集器
 *
 * @author luming
 * @date 2022/4/11
 */
@Slf4j
public class DorisMetadataCollect extends RdbmsMetaDataCollect {
    /**
     * http sourceDTO
     */
    private DorisRestfulSourceDTO restSource;
    /**
     * http请求的ip和端口url，后续source里的url会被重写用于构建httpClient
     */
    private String originUrl = "";

    @Override
    protected void openConnection() {
        //doris 需要jdbc和http两种方式获取元数据，分别构建两种source，且都需要支持多节点连接
        DorisSourceDTO dorisSourceDTO = (DorisSourceDTO) sourceDTO;

        //这里必须使用深拷贝
        restSource = (DorisRestfulSourceDTO) dorisSourceDTO.getRestfulSource().clone();
        getConnectedUrl(restSource.getUrl());

        if (StringUtils.isBlank(restSource.getCluster())) {
            restSource.setCluster(DEFAULT_CLUSTER);
        }

        //获取jdbc连接并补充当前db下的所有表
        super.openConnection();
    }

    @Override
    protected List<Object> showTables() {
        restSource.setUrl(originUrl +
                String.format
                        (DorisConstants.QUERY_TABLE_LIST, restSource.getCluster(), currentDatabase));

        ListResponse res = doHttp(ListResponse.class);
        if (SUCCESS_FLAG.equalsIgnoreCase(res.getMsg())) {
            return new ArrayList<>(res.getList());
        }
        return Lists.newArrayList();
    }

    @Override
    protected MetadataRdbEntity createMetadataRdbEntity() {
        MetadataDorisEntity entity = new MetadataDorisEntity();
        String currentTable = String.valueOf(currentObject);

        try {
            List<DorisIndexEntity> indexEntities = queryIndexes(
                    currentDatabase, currentTable);
            List<DorisColumnEntity> columnEntities = queryColumn(
                    restSource.getCluster(), currentDatabase, currentTable);
            Map<String, DorisColumnMetaEntity> columnMetaEntities = getColumnMetadata(
                    currentDatabase, currentTable);
            columnEntities.forEach(item -> {
                String name = item.getName();
                DorisColumnMetaEntity meta = columnMetaEntities.get(name);
                item.setDigital(meta.getDecimalDigits());
                item.setPrecision(meta.getColumnSize());
            });

            DorisTableEntity tableEntity = createTableMeta(currentDatabase, currentTable);
            entity.setSchema(currentDatabase);
            entity.setTableName(currentTable);
            entity.setColumns(columnEntities);
            entity.setIndexEntities(indexEntities);
            entity.setTableProperties(tableEntity);
            entity.setQuerySuccess(true);
        } catch (Exception e) {
            throw new DtLoaderException("query doris metadata error", e);
        }
        return entity;
    }

    /**
     * 创建表元数据信息对象
     *
     * @param database db
     * @param table    table
     * @return entity
     */
    private DorisTableEntity createTableMeta(String database, String table) throws Exception {
        DorisTableEntity tableEntity = queryTableInformation(database, table);
        long rowCount = queryTableRowCount(database, table);
        tableEntity.setRows(rowCount);
        return tableEntity;
    }

    /**
     * 查询指定表的数据条数
     *
     * @param database 指定库名
     * @param table    指定表名
     * @return 当前表下的数据条数
     */
    private long queryTableRowCount(String database, String table) {
        restSource.setUrl(originUrl +
                String.format(DorisConstants.QUERY_ROW_COUNT, database, table));

        RowCountResponse response = doHttp(RowCountResponse.class);
        if (SUCCESS_FLAG.equalsIgnoreCase(response.getMsg())) {
            return response.getRowCount(table);
        }
        return -1;
    }

    /**
     * 查询doris information_schema下 tables表，获取对应table的详细元数据
     * 注意：这个方法在某些情况的时候，会出现卡死现象，官方不建议查询information_schema
     *
     * @param database 指定的database
     * @param table    指定的table
     * @return 指定表对应的详细信息
     */
    private DorisTableEntity queryTableInformation(String database, String table) throws Exception {
        String sql = String.format(DorisConstants.SQL_SHOW_INFORMATION, database, table);
        ResultSet rs = statement.executeQuery(sql);
        return JsonUtils.toJavaBean(rs, DorisTableEntity.class);
    }

    /**
     * 查询指定数据库和表名的字段信息
     *
     * @param cluster  指定的集群名称
     * @param database 指定的数据库
     * @param table    指定的表名
     * @return 字段信息列表
     */
    private List<DorisColumnEntity> queryColumn(String cluster, String database, String table) {
        restSource.setUrl(originUrl
                + String.format(DorisConstants.QUERY_TABLE_INFO, cluster, database, table));

        SchemaResponse response = doHttp(SchemaResponse.class);
        if (SUCCESS_FLAG.equalsIgnoreCase(response.getMsg())) {
            return response.getData().get(table).getColumns();
        }
        return Lists.newArrayList();
    }

    private Map<String, DorisColumnMetaEntity> getColumnMetadata(
            String databaseName, String tableName) {
        Map<String, DorisColumnMetaEntity> columnMetaEntityMap = new HashMap<>();
        try (ResultSet resultSet =
                     connection
                             .getMetaData()
                             .getColumns(databaseName, null, tableName, null)) {
            while (resultSet.next()) {
                ResultSetMetaData metaData = resultSet.getMetaData();
                int columnCount = metaData.getColumnCount();
                Map<String, Object> metadataMap = new HashMap<>();
                for (int i = 1; i < columnCount + 1; i++) {
                    String name = metaData.getColumnName(i);
                    String value = resultSet.getString(name);
                    metadataMap.put(name, value);
                }
                String toJson = JSON.toJSONString(metadataMap);
                DorisColumnMetaEntity entity = JSON.parseObject(toJson, DorisColumnMetaEntity.class);
                columnMetaEntityMap.put(entity.getColumnName(), entity);
            }
        } catch (Exception e) {
            throw new DtLoaderException("Get metadata of column from result failed.", e);
        }
        return columnMetaEntityMap;
    }

    /**
     * 查询当前表下索引的元数据信息
     *
     * @param database 指定库名
     * @param table    指定表名
     * @return 索引元数据信息
     */
    private List<DorisIndexEntity> queryIndexes(String database, String table) throws Exception {
        String sql = String.format(
                DorisConstants.SQL_SHOW_INDEX, database, table);
        ResultSet rs = statement.executeQuery(sql);
        return JsonUtils.toJavaBeanList(rs, DorisIndexEntity.class);
    }

    @Override
    protected ConnFactory getConnFactory() {
        return new DorisConnFactory();
    }

    /**
     * 进行http请求，并返回相应的class对象
     *
     * @param clazz class泛型
     * @return class实例对象
     */
    private <T> T doHttp(Class<T> clazz) {
        try (HttpClient httpClient =
                     HttpClientFactory.createHttpClientAndStart(restSource)) {
            Response result = httpClient.get();
            AssertUtils.isTrue(result, 0);
            return JSON.parseObject(result.getContent(), clazz);
        } catch (IOException e) {
            throw new DtLoaderException(e.getMessage(), e);
        }
    }

    /**
     * 获取restful 成功连接的url，任一节点成功即可
     *
     * @param urls 多节点url
     */
    private void getConnectedUrl(String urls) {
        AssertUtils.notBlank(urls, "url can't be null");

        Exception lastException = null;
        for (String url : urls.split(",")) {
            originUrl = url;
            try {
                showTables();
                return;
            } catch (Exception e) {
                lastException = e;
                log.error("dorisRestful connect error.", e);
            }
        }

        //全部节点都连接失败则抛出最后一个节点的失败信息
        String message = ExceptionUtils.getRootCause(lastException).getMessage();
        throw new DtLoaderException("no url available ,last exception : " + message, lastException);
    }
}
