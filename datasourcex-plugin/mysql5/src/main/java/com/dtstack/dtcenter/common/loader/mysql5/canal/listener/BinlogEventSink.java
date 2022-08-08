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

package com.dtstack.dtcenter.common.loader.mysql5.canal.listener;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.common.AbstractCanalLifeCycle;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.sink.CanalEventSink;
import com.alibaba.otter.canal.sink.exception.CanalSinkException;
import com.dtstack.dtcenter.loader.dto.canal.BinlogCallBackLogDTO;
import com.dtstack.dtcenter.loader.dto.canal.ColumnEntity;
import com.dtstack.dtcenter.common.loader.common.utils.SnowflakeIdWorker;
import lombok.extern.slf4j.Slf4j;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * bin log事件监听处理
 *
 * @author zhiyi
 */
@Slf4j
public class BinlogEventSink extends AbstractCanalLifeCycle implements CanalEventSink<List<CanalEntry.Entry>> {

    private BlockingQueue<Object> queue;

    private SnowflakeIdWorker idWorker;

    private static final String HEART_BEAT_ERROR_START = "HeartBeat";

    /**
     * 监听事件类型
     */
    private List<String> categories = new ArrayList<>();

    /**
     * 附加参数，回调原封返回
     */
    private Map<Object, Object> otherParams;

    public BinlogEventSink() {
        queue = new LinkedBlockingQueue<>();
        idWorker = new SnowflakeIdWorker(1, 1);
    }

    @Override
    public boolean sink(List<CanalEntry.Entry> entries, InetSocketAddress inetSocketAddress, String destination) throws CanalSinkException {
        for (CanalEntry.Entry entry : entries) {
            CanalEntry.EntryType entryType = entry.getEntryType();
            if (entryType != CanalEntry.EntryType.ROWDATA) {
                continue;
            }
            if (log.isDebugEnabled()) {
                log.debug("binlog sink, entryType:{}", entry.getEntryType());
            }
            CanalEntry.RowChange rowChange = parseRowChange(entry);
            if (rowChange == null) {
                return false;
            }
            CanalEntry.Header header = entry.getHeader();
            String schema = header.getSchemaName();
            String table = header.getTableName();
            processRowChange(rowChange, schema, table);
        }
        return true;
    }

    private CanalEntry.RowChange parseRowChange(CanalEntry.Entry entry) {
        CanalEntry.RowChange rowChange = null;
        try {
            rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
        } catch (Exception e) {
            log.error("ERROR ## parseRowChange has an error , data:" + entry.toString());
        }
        return rowChange;
    }

    /**
     * 处理变更的table行数据
     */
    private void processRowChange(CanalEntry.RowChange rowChange, String schema, String table) {
        CanalEntry.EventType eventType = rowChange.getEventType();
        if (!accept(eventType.toString())) {
            return;
        }
        for (CanalEntry.RowData rowData : rowChange.getRowDatasList()) {
            Map<String, ColumnEntity> map = new HashMap<>(8);
            List<CanalEntry.Column> beforeColumnsList = rowData.getBeforeColumnsList();
            if (!beforeColumnsList.isEmpty()) {
                for (CanalEntry.Column column : beforeColumnsList) {
                    if (column.getIsNull()) {
                        continue;
                    }
                    ColumnEntity columnEntity = ColumnEntity.builder()
                            .name(column.getName())
                            .beforeValue(column.getValue())
                            .updated(column.getUpdated())
                            .mysqlType(column.getMysqlType())
                            .build();
                    map.put(column.getName(), columnEntity);
                }
            }
            List<CanalEntry.Column> afterColumnsList = rowData.getAfterColumnsList();
            if (!afterColumnsList.isEmpty()) {
                for (CanalEntry.Column column : afterColumnsList) {
                    if (column.getIsNull()) {
                        continue;
                    }
                    ColumnEntity columnEntity = map.get(column.getName());
                    if (columnEntity != null) {
                        columnEntity.setAfterValue(column.getValue());
                        columnEntity.setUpdated(column.getUpdated());
                    } else {
                        ColumnEntity columnEntity2 = ColumnEntity.builder()
                                .name(column.getName())
                                .afterValue(column.getValue())
                                .updated(column.getUpdated())
                                .mysqlType(column.getMysqlType())
                                .build();
                        map.put(column.getName(), columnEntity2);
                    }
                }
            }
            BinlogCallBackLogDTO callBackLog = BinlogCallBackLogDTO.builder()
                    .ts(idWorker.nextId())
                    .schema(schema)
                    .table(table)
                    .eventType(eventType.toString())
                    .columnEntities(map.values())
                    .otherParams(otherParams)
                    .build();
            try {
                queue.put(callBackLog);
            } catch (InterruptedException e) {
                log.error("takeEvent interrupted message:{} error:{}", JSONObject.toJSONString(callBackLog), e);
            }
        }

    }

    /**
     * 判断是否是的监听事件类型
     * 未设置则监听全部事件类型
     */
    private boolean accept(String type) {
        return categories.isEmpty() || categories.contains(type);
    }

    public void processEvent(Object event) {
        try {
            queue.put(event);
        } catch (InterruptedException e) {
            log.error("takeEvent interrupted event:{} error:{}", event.toString(), e.getMessage());
        }
    }

    /**
     * 获取变更数据
     *
     * @return 变更数据
     */
    public BinlogCallBackLogDTO takeEvent() {
        try {
            Object msg = queue.take();
            // heartbeatController中的错误消息在此处处理
            if (msg instanceof String && ((String) msg).startsWith(HEART_BEAT_ERROR_START)) {
                throw new RuntimeException(msg.toString());
            }
            if (msg instanceof BinlogCallBackLogDTO) {
                return (BinlogCallBackLogDTO) msg;
            }
        } catch (InterruptedException e) {
            log.error("takeEvent interrupted error:{}", e.getMessage());
        }
        return null;
    }

    @Override
    public void interrupt() {
        this.stop();
        log.info("BinlogEventSink is interrupted");
    }

    public void setOtherParams(Map<Object, Object> otherParams) {
        this.otherParams = otherParams;
    }

    public void setCategories(List<String> categories) {
        this.categories = categories;
    }
}
