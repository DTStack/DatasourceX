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

package com.dtstack.dtcenter.common.loader.hivecdc.metadata.client;

import com.alibaba.fastjson.JSON;
import com.dtstack.dtcenter.common.loader.hivecdc.metadata.collect.HiveCdcMetadataCollectManager;
import com.dtstack.dtcenter.common.loader.hivecdc.metadata.entity.DdlType;
import com.dtstack.dtcenter.common.loader.hivecdc.metadata.entity.HiveTableEntity;
import com.dtstack.dtcenter.common.loader.hivecdc.metadata.entity.MetadataHiveCdcEntity;
import com.dtstack.dtcenter.common.loader.hivecdc.metadata.entity.QueueData;
import com.dtstack.dtcenter.common.loader.hivecdc.metadata.factory.HiveCdcConFactory;
import com.dtstack.dtcenter.common.loader.hivecdc.metadata.util.HiveDriverUtils;
import com.dtstack.dtcenter.common.loader.metadata.constants.BaseCons;
import com.dtstack.dtcenter.common.loader.rdbms.AbsRdbmsClient;
import com.dtstack.dtcenter.common.loader.rdbms.ConnFactory;
import com.dtstack.dtcenter.loader.cache.client.ClassLoaderCache;
import com.dtstack.dtcenter.loader.dto.metadata.entity.rdb.ColumnEntity;
import com.dtstack.dtcenter.loader.dto.source.HiveCdcSourceDTO;
import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.dtstack.dtcenter.loader.metadata.MetaDataCollectManager;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.NotificationEvent;
import org.apache.hadoop.hive.metastore.api.NotificationEventResponse;
import org.apache.hadoop.hive.metastore.api.Table;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.dtstack.dtcenter.common.loader.hivecdc.metadata.cons.HiveMetaCons.KEY_RESULTSET_COMMENT;
import static com.dtstack.dtcenter.common.loader.hivecdc.metadata.cons.HiveMetaCons.KEY_TRANSIENT_LASTDDLTIME;

/**
 * hiveCdc client
 *
 * @author luming
 * @date 2022/4/15
 */
@Slf4j
public class HiveCdcClient extends AbsRdbmsClient implements CdcJob {

    private HiveMetaStoreClient metaClient;

    private BlockingQueue<QueueData> dataQueue = new LinkedBlockingQueue<>();

    private Set<String> excludeDatabase;

    private List<Pattern> excludeTable;

    private List<Pattern> includeTableList;

    private long startId;

    public HiveCdcClient() {
    }

    public HiveCdcClient(BlockingQueue<QueueData> queue) {
        dataQueue = queue;
    }

    @Override
    public void run() {
        collector();
    }

    @Override
    public void init(ISourceDTO sourceDTO,
                     List<String> excludeDatabase,
                     List<String> excludeTable,
                     List<Map<String, Object>> originalJob) {
        HiveCdcSourceDTO source = (HiveCdcSourceDTO) sourceDTO;

        try {
            ClassLoader classLoader = ClassLoaderCache.getClassLoader(DataSourceType.HIVE_CDC.getPluginName());
            Thread.currentThread().setContextClassLoader(classLoader);

            metaClient = HiveDriverUtils.getMetaStoreClient(source.getMetaStoreUris(), source.getKerberosConfig());
            startId = metaClient.getCurrentNotificationEventId().getEventId();
            this.excludeDatabase = CollectionUtils.isEmpty(excludeDatabase)
                    ? Collections.emptySet()
                    : new HashSet<>(excludeDatabase);
            this.excludeTable = CollectionUtils.isNotEmpty(excludeTable) ?
                    excludeTable.stream().map(Pattern::compile).collect(Collectors.toList())
                    : Collections.emptyList();

            if (CollectionUtils.isNotEmpty(originalJob)) {
                ArrayList<Pattern> includeTables = new ArrayList<>();
                for (Map<String, Object> jobMap : originalJob) {
                    String dbName = MapUtils.getString(jobMap, BaseCons.KEY_DB_NAME);
                    if (org.apache.commons.lang.StringUtils.isNotEmpty(dbName)) {
                        List<Object> tables = (List<Object>) jobMap.get(BaseCons.KEY_TABLE_LIST);
                        if (CollectionUtils.isNotEmpty(tables)) {
                            tables.forEach(t -> includeTables.add(Pattern.compile(dbName + "." + t)));
                        } else {
                            includeTables.add(Pattern.compile(dbName + ".*"));
                        }
                    }
                }
                this.includeTableList = includeTables;
            }

        } catch (Exception e) {
            throw new DtLoaderException("hive cdc client init error. ", e);
        }
    }

    @Override
    public void collector() {
        while (true) {
            try {
                long eventId = metaClient.getCurrentNotificationEventId().getEventId();
                if (eventId != startId) {
                    NotificationEventResponse nextNotification = metaClient.getNextNotification(startId, 100, null);

                    List<NotificationEvent> events = nextNotification.getEvents();

                    if (CollectionUtils.isNotEmpty(events)) {
                        for (NotificationEvent event : events) {
                            if (!filterEvent(event)) {
                                fillEntry(event);
                            }
                            startId = event.getEventId();
                        }
                    }
                }

                TimeUnit.SECONDS.sleep(5);
            } catch (Exception e) {
                //直接进行外部进行重试
                log.warn("collector failed ", e);
                try {
                    dataQueue.put(new QueueData(e));
                } catch (InterruptedException e1) {
                    log.warn("put data failed", e1);
                }
            }
        }
    }

    @Override
    protected ConnFactory getConnFactory() {
        return new HiveCdcConFactory();
    }

    @Override
    protected DataSourceType getSourceType() {
        return DataSourceType.HIVE_CDC;
    }

    @Override
    public MetaDataCollectManager getMetadataCollectManager(ISourceDTO sourceDTO) {
        return new HiveCdcMetadataCollectManager(sourceDTO);
    }

    /**
     * 监听是监听所有的库表，所以需要根据监听的表名和库名进行过滤
     * 如果仅仅是表条数增加 减少 其他不变 也需要过滤掉
     *
     * @param event 监听事件
     * @return 是否填充成功
     */
    private boolean filterEvent(NotificationEvent event) {

        String dbName = event.getDbName();
        //hive1.1.1版本 create_database 事件，db字段为空  只能从message里去拿db
        if (dbName == null) {
            if (event.getEventType().equals("CREATE_DATABASE")) {
                Map map = JSON.parseObject(event.getMessage(), Map.class);
                if (null != map.get("db")) {
                    event.setDbName(map.get("db").toString());
                    dbName = event.getDbName();
                }
            }
        }

        if (!event.isSetTableName()) {
            //hive1.1.1版本 CREATE_TABLE 事件，table字段为空  只能从message里去拿table
            if (event.getEventType().equals("CREATE_TABLE")) {
                Map map = JSON.parseObject(event.getMessage(), Map.class);
                if (null != map.get("table")) {
                    event.setTableName(map.get("table").toString());
                }
            }
        }

        //监听的表没匹配到需要过滤
        if (event.isSetDbName()
                && event.isSetTableName()
                && CollectionUtils.isNotEmpty(includeTableList)
                && includeTableList.stream().noneMatch(i -> i.matcher(event.getDbName() + "." + event.getTableName()).matches())) {
            return true;
        }

        //database没有过滤条件 即对table进行过滤
        if (event.isSetTableName()
                && CollectionUtils.isEmpty(excludeDatabase)
                && excludeTable.stream().anyMatch(i -> i.matcher(event.getTableName()).matches())) {
            log.debug("filter eventId {}, eventType {} ,dbName is {}, tableName is {} ", event.getEventId(), event.getEventType(), dbName, event.getTableName());
            return true;
        }

        //过滤的database集合不为空 匹配则过滤
        //如果过滤的database集合和table集合都不为空 则对schema.table进行匹配
        if (excludeDatabase.contains(dbName)) {
            if (CollectionUtils.isEmpty(excludeTable)) {
                log.debug("filter eventId {}, eventType {} ,dbName is {}, tableName is {} ", event.getEventId(), event.getEventType(), dbName, event.getTableName());
                return true;
            } else {
                if (event.isSetTableName()
                        && excludeTable.stream().anyMatch(i -> i.matcher(event.getTableName()).matches())) {
                    log.debug("filter eventId {}, eventType {} ,dbName is {}, tableName is {} ", event.getEventId(), event.getEventType(), dbName, event.getTableName());
                    return true;
                }
            }
        }

        //如果仅仅只是rowsize发生了变化直接也需要过滤掉 分区表暂时无法判断
        Map<String, Object> a = JSON.parseObject(event.getMessage(), Map.class);
        Object tableObjBeforeJson = a.get("tableObjBeforeJson");
        Object tableObjAfterJson = a.get("tableObjAfterJson");
        if (tableObjBeforeJson != null && tableObjAfterJson != null) {
            int tableObjBeforeJsonIndex = tableObjBeforeJson.toString().indexOf("transient_lastDdlTime");
            int tableObjAfterJsonIndex = tableObjBeforeJson.toString().indexOf("transient_lastDdlTime");
            if (tableObjBeforeJsonIndex - 2 != -1 && tableObjAfterJsonIndex - 2 != -1) {
                String s = tableObjBeforeJson.toString().substring(0, tableObjBeforeJsonIndex - 2) + tableObjBeforeJson.toString().substring(tableObjBeforeJsonIndex + 1 + "transient_lastDdlTime".length() + 13);
                String s1 = tableObjAfterJson.toString().substring(0, tableObjBeforeJsonIndex - 2) + tableObjAfterJson.toString().substring(tableObjBeforeJsonIndex + 1 + "transient_lastDdlTime".length() + 13);
                if (s.equals(s1)) {
                    return true;
                }
                try {
                    //切割的数据可能不是json格式的
                    Map beforeMap = JSON.parseObject(s, Map.class);
                    Map afterMap = JSON.parseObject(s1, Map.class);

                    //9对应的值是rowdatasize这种数量变化 不需要比较
                    beforeMap.remove("9");
                    afterMap.remove("9");
                    return beforeMap.toString().equals(afterMap.toString());
                } catch (Exception e) {
                    //ignore exception
                }
            }
        }
        return false;
    }

    /**
     * 填充事件entry
     *
     * @param event 监听到的事件
     */
    private void fillEntry(NotificationEvent event) {
        MetadataHiveCdcEntity metadataEntity = new MetadataHiveCdcEntity();
        metadataEntity.setSchema(event.getDbName());
        metadataEntity.setTableName(event.getTableName());

        String eventType = event.getEventType();
        DdlType ddlType = DdlType.valuesOf(eventType);

        try {
            Table table = null;
            if (event.getDbName() != null && event.getTableName() != null && ddlType != DdlType.DROP_TABLE) {
                table = metaClient.getTable(event.getDbName(), event.getTableName());
            }
            //alter.create类型的需要进行填充字段信息
            if (!ddlType.getType().equals("DROP") && null != table) {
                List<ColumnEntity> hiveColumnEntities = transferColumn(table.getPartitionKeys());
                metadataEntity.setPartitionColumns(hiveColumnEntities);
                metadataEntity.setColumns(transferColumn(table.getSd().getCols()));
                metadataEntity.setPartitions(getPartitions(hiveColumnEntities));
            }

            if (fillRenameOperate(event, ddlType, metadataEntity)) {
                ddlType = DdlType.RENAME_TABLE;
            }

            //组装表的参数信息
            if (null != table) {
                HiveTableEntity hiveTableEntity = generateTableInfo(table);
                if (hiveTableEntity.getLastAccessTime() == 0L) {
                    hiveTableEntity.setLastAccessTime((long) event.getEventTime() * 1000L);
                }
                metadataEntity.setTableProperties(hiveTableEntity);
            }
            metadataEntity.setQuerySuccess(true);
            metadataEntity.setOperaType(ddlType.getType());
            metadataEntity.setDdlType(ddlType.name());
        } catch (Exception e) {
            metadataEntity.setQuerySuccess(false);
            metadataEntity.setErrorMsg(e.getMessage());
            metadataEntity.setDdlType(ddlType.name());
        }
        try {
            dataQueue.put(new QueueData(metadataEntity, event.getEventId()));
        } catch (InterruptedException e) {
            log.warn("put data failed", e);
        }

    }

    private boolean fillRenameOperate(NotificationEvent event, DdlType ddlType, MetadataHiveCdcEntity metadataEntity) {
        // rename 还要加上一个oldName
        if (ddlType == DdlType.ALTER_TABLE) {
            Map<String, Object> message = JSON.parseObject(event.getMessage(), Map.class);
            Object tableObjBeforeJson = message.get("tableObjBeforeJson");
            Object tableObjAfterJson = message.get("tableObjAfterJson");

            if (tableObjBeforeJson != null && tableObjAfterJson != null) {
                Map beforeMap = JSON.parseObject(tableObjBeforeJson.toString(), Map.class);
                Map afterMap = JSON.parseObject(tableObjAfterJson.toString(), Map.class);
                Object o = beforeMap.get("1");
                Object o1 = afterMap.get("1");
                if ((o != null && o1 != null) && (o instanceof Map && o1 instanceof Map)) {
                    Object beforeName = ((Map<String, Object>) o).get("str");
                    Object afterName = ((Map<String, Object>) o1).get("str");
                    if ((beforeName != null && afterName != null) && !beforeName.toString().equals(afterName.toString())) {
                        metadataEntity.setOlderTableName(beforeName.toString());
                        return true;
                    }
                }
            } else if (message.containsKey("table") && !message.get("table").toString().equals(event.getTableName())) {
                // hive1.1.1以及以下版本没有before 和after信息
                metadataEntity.setOlderTableName(message.get("table").toString());
                return true;
            }
        }
        return false;
    }

    /**
     * 字段属性转化
     *
     * @param cols
     * @return
     */
    private List<ColumnEntity> transferColumn(List<FieldSchema> cols) {
        List<ColumnEntity> hiveColumnEntities = Lists.newArrayList();
        if (CollectionUtils.isEmpty(cols)) {
            return hiveColumnEntities;
        }
        int fieldIndex = 1;
        for (FieldSchema fieldSchema : cols) {
            ColumnEntity hiveColumnEntity = new ColumnEntity();
            hiveColumnEntity.setType(fieldSchema.getType());
            hiveColumnEntity.setName(fieldSchema.getName());
            hiveColumnEntity.setComment(fieldSchema.getComment() == null ? "" : fieldSchema.getComment());
            if (fieldSchema.getType().contains("(") && fieldSchema.getType().contains(")")) {
                int precisionEndPosition = !fieldSchema.getType().contains(",") ? fieldSchema.getType().indexOf(")") : fieldSchema.getType().indexOf(",");
                String precision = StringUtils.substring(fieldSchema.getType(), fieldSchema.getType().indexOf("(") + 1, precisionEndPosition);
                hiveColumnEntity.setPrecision(Integer.parseInt(precision));
                if (precisionEndPosition < fieldSchema.getType().length() - 1) {
                    hiveColumnEntity.setDigital(Integer.parseInt(StringUtils.substring(fieldSchema.getType(), precisionEndPosition + 1, fieldSchema.getType().length() - 1)));
                }
            }
            hiveColumnEntity.setIndex(fieldIndex);
            fieldIndex++;
            hiveColumnEntities.add(hiveColumnEntity);
        }
        return hiveColumnEntities;
    }

    private List<String> getPartitions(List<ColumnEntity> columnEntityList) {
        ArrayList<String> partitionColumns = new ArrayList<>();
        for (ColumnEntity columnEntity : columnEntityList) {
            partitionColumns.add(columnEntity.getName());
        }
        return partitionColumns;
    }

    /**
     * 表参数转化
     *
     * @param table
     * @return
     */
    private HiveTableEntity generateTableInfo(Table table) {
        HiveTableEntity hiveTableProperties = new HiveTableEntity();
        hiveTableProperties.setComment(table.getParameters().getOrDefault(KEY_RESULTSET_COMMENT, ""));
        hiveTableProperties.setLocation(table.getSd().getLocation());
        hiveTableProperties.setCreateTime((long) table.getCreateTime() * 1000L);
        hiveTableProperties.setLastAccessTime((long) table.getLastAccessTime() * 1000L);
        hiveTableProperties.setTransientLastDdlTime(Long.parseLong(table.getParameters().get(KEY_TRANSIENT_LASTDDLTIME)) * 1000L);
        hiveTableProperties.setStoreType(HiveDriverUtils.getStoredType(table.getSd().getOutputFormat()));
        return hiveTableProperties;
    }
}
