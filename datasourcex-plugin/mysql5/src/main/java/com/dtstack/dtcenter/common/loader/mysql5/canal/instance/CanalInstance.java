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

package com.dtstack.dtcenter.common.loader.mysql5.canal.instance;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.filter.aviater.AviaterRegexFilter;
import com.alibaba.otter.canal.parse.inbound.mysql.MysqlEventParser;
import com.alibaba.otter.canal.parse.index.MemoryLogPositionManager;
import com.alibaba.otter.canal.parse.support.AuthenticationInfo;
import com.alibaba.otter.canal.protocol.position.EntryPosition;
import com.dtstack.dtcenter.common.loader.common.utils.BinlogUtil;
import com.dtstack.dtcenter.common.loader.common.utils.RetryUtil;
import com.dtstack.dtcenter.common.loader.common.utils.TelnetUtil;
import com.dtstack.dtcenter.common.loader.mysql5.canal.listener.BinlogAlarmHandler;
import com.dtstack.dtcenter.common.loader.mysql5.canal.listener.BinlogEventSink;
import com.dtstack.dtcenter.common.loader.mysql5.canal.listener.BinlogJournalValidator;
import com.dtstack.dtcenter.common.loader.mysql5.canal.listener.HeartBeatController;
import com.dtstack.dtcenter.loader.dto.canal.BinlogCallBackLogDTO;
import com.dtstack.dtcenter.loader.dto.canal.BinlogConfigDTO;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;

import java.net.InetSocketAddress;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static com.dtstack.dtcenter.loader.dto.canal.BinlogConstant.BIN_LOG_CONFIG_NOT_RIGHT_TIP_TEMPLATE;
import static com.dtstack.dtcenter.loader.dto.canal.BinlogConstant.BIN_LOG_FORMAT_TIP;
import static com.dtstack.dtcenter.loader.dto.canal.BinlogConstant.BIN_LOG_NOT_ENABLED_TIP;
import static com.dtstack.dtcenter.loader.dto.canal.BinlogConstant.BIN_LOG_START_POSITION_TIP_TEMPLATE;
import static com.dtstack.dtcenter.loader.dto.canal.BinlogConstant.BIN_LOG_START_TIMESTAMP_TIP_TEMPLATE;
import static com.dtstack.dtcenter.loader.dto.canal.BinlogConstant.BIN_LOG_TIP_TEMPLATE;
import static com.dtstack.dtcenter.loader.dto.canal.BinlogConstant.DEFAULT_CAT;
import static com.dtstack.dtcenter.loader.dto.canal.BinlogConstant.DEFAULT_DESTINATION;
import static com.dtstack.dtcenter.loader.dto.canal.BinlogConstant.ERROR_CHECK_BIN_LOG_TEMPLATE;
import static com.dtstack.dtcenter.loader.dto.canal.BinlogConstant.JDBC_URL_TEMPLATE;
import static com.dtstack.dtcenter.loader.dto.canal.BinlogConstant.JOURNAL_NAME;
import static com.dtstack.dtcenter.loader.dto.canal.BinlogConstant.NO_HOST_TIP;
import static com.dtstack.dtcenter.loader.dto.canal.BinlogConstant.NO_URL_TIP;
import static com.dtstack.dtcenter.loader.dto.canal.BinlogConstant.NO_USERNAME_TIP;
import static com.dtstack.dtcenter.loader.dto.canal.BinlogConstant.POSITION;
import static com.dtstack.dtcenter.loader.dto.canal.BinlogConstant.SCHEMA_FORMAT;
import static com.dtstack.dtcenter.loader.dto.canal.BinlogConstant.SCHEMA_TABLE_FORMAT;
import static com.dtstack.dtcenter.loader.dto.canal.BinlogConstant.TIMESTAMP;
import static com.dtstack.dtcenter.loader.dto.canal.BinlogConstant.USER_PRIVILEGE_TIP_TEMPLATE;
import static com.dtstack.dtcenter.loader.dto.canal.BinlogConstant.USER_SELECT_PRIVILEGE_TEMPLATE;


/**
 * Canal实例
 *
 * @author by zhiyi
 * @date 2022/3/18 11:08 上午
 */
@Slf4j
public class CanalInstance {

    private static final ExecutorService singleExecutorService = new ThreadPoolExecutor(1, 1,
            0L, TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<>(), r -> new Thread(r, "canal_listener_thread"));

    private BinlogConfigDTO binlogConfig;

    private BinlogEventSink eventSink;

    private MysqlEventParser controller;


    /**
     * 实例是否已启动
     */
    private AtomicBoolean isStart = new AtomicBoolean(false);

    public CanalInstance(BinlogConfigDTO binlogConfig) {
        this.binlogConfig = binlogConfig;
    }


    /**
     * 解析置监听的库表信息，构造filter
     * 1.  所有表：.*   or  .*\\..*
     * 2.  canal schema下所有表： canal\\..*
     * 3.  canal下的以canal打头的表：canal\\.canal.*
     * 4.  canal schema下的一张表：canal\\.test1
     * 5.  多个规则组合使用：canal\\..*,mysql.test1,mysql.test2 (逗号分隔)
     */
    private void formatFilter() {
        Set<String> set = new HashSet<>();
        Map<String, List<String>> filterDb = binlogConfig.getFilterDb();
        if (filterDb != null && !filterDb.isEmpty()) {
            Set<String> schemas = filterDb.keySet();
            for (String schema : schemas) {
                List<String> list = filterDb.get(schema);
                if (!list.isEmpty()) {
                    list.forEach(table -> set.add(String.format(SCHEMA_TABLE_FORMAT, schema, table)));
                } else {
                    set.add(String.format(SCHEMA_FORMAT, schema));
                }
            }
        }
        if (StringUtils.isNotBlank(binlogConfig.getFilter())) {
            String filter = binlogConfig.getFilter();
            String[] split = filter.split(",");
            set.addAll(Arrays.asList(split));
        }
        binlogConfig.setFilter(String.join(",", set));
        log.info("------------filter: " + binlogConfig.getFilter() + "----------");
    }

    /**
     * 启动canal监听实例
     */
    public void start() {
        if (isStart.get()) {
            return;
        }
        beforeStart();
        MysqlEventParser controller = new MysqlEventParser();
        controller.setConnectionCharset(Charset.forName(binlogConfig.getConnectionCharset()));
        controller.setSlaveId(binlogConfig.getSlaveId());
        controller.setDetectingEnable(binlogConfig.getDetectingEnable());
        controller.setDetectingSQL(binlogConfig.getDetectingSql());
        controller.setMasterInfo(new AuthenticationInfo(new InetSocketAddress(binlogConfig.getHost(), binlogConfig.getPort()), binlogConfig.getUsername(), binlogConfig.getPassword()));
        controller.setEnableTsdb(binlogConfig.getEnableTsdb());
        controller.setDestination(DEFAULT_DESTINATION);
        controller.setParallel(binlogConfig.getParallel());
        controller.setParallelBufferSize(binlogConfig.getBufferSize());
        controller.setParallelThreadSize(binlogConfig.getParallelThreadSize());
        controller.setIsGTIDMode(binlogConfig.getIsGtidMode());
        BinlogAlarmHandler alarmHandler = new BinlogAlarmHandler();
        alarmHandler.setExit(binlogConfig.getExit());
        alarmHandler.setEventParser(controller);
        controller.setAlarmHandler(alarmHandler);
        controller.setTransactionSize(binlogConfig.getTransactionSize());
        BinlogEventSink sink = new BinlogEventSink();
        sink.setOtherParams(binlogConfig.getOtherParams());
        if (StringUtils.isNotBlank(binlogConfig.getCat())) {
            sink.setCategories(Arrays.asList(binlogConfig.getCat().toUpperCase().split(",")));
        }
        this.eventSink = sink;
        sink.start();
        controller.setEventSink(sink);
        MemoryLogPositionManager positionManager = new MemoryLogPositionManager();
        positionManager.start();
        controller.setLogPositionManager(positionManager);
        // 添加connection心跳回调处理器
        HeartBeatController heartBeatController = new HeartBeatController();
        heartBeatController.setBinlogEventSink(sink);
        controller.setHaController(heartBeatController);
        if (StringUtils.isNotEmpty(binlogConfig.getFilter())) {
            controller.setEventFilter(new AviaterRegexFilter(binlogConfig.getFilter()));
        }
        EntryPosition startPosition = findStartPosition();
        if (startPosition != null) {
            controller.setMasterPosition(startPosition);
        }
        controller.start();
        this.controller = controller;
        isStart.set(true);
        log.info("canal instance start, binlog config info:" + JSONObject.toJSONString(binlogConfig));
    }

    private void beforeStart() {
        // 构造filter
        formatFilter();
        // 校验参数是否正确
        checkConfig();
    }

    /**
     * 终止canal实例
     */
    public void stop() {
        try {
            if (isStart.get() && controller != null) {
                controller.stop();
                isStart.set(false);
            }
        } catch (Exception e) {
            log.error("stop canal instance error: ", e);
        }
    }

    /**
     * 获取canal监听binlog变更信息
     *
     * @return binlog变更信息
     */
    public BinlogCallBackLogDTO getCallBackLog() {
        if (!isStart.get()) {
            throw new RuntimeException("canal instance not start");
        }
        if (eventSink != null && eventSink.isStart()) {
            return eventSink.takeEvent();
        }
        return null;
    }

    /**
     * canal binlog监听处理方法
     *
     * @param consumer 具体的日志处理逻辑
     */
    public void listener(Consumer<BinlogCallBackLogDTO> consumer) {
        singleExecutorService.execute(() -> {
            log.info("----------listener start----------");
            while (isStart.get()) {
                try {
                    BinlogCallBackLogDTO callBackLog = getCallBackLog();
                    if (callBackLog != null) {
                        log.info("binlog call back: {}", callBackLog.toString());
                        consumer.accept(callBackLog);
                    }
                } catch (Exception e) {
                    throw new RuntimeException("consumer canal binlog call back log error", e);
                }
            }

        });
    }

    /**
     * 校验binlog配置是否正确
     */
    private void checkConfig() {
        StringBuilder sb = new StringBuilder(256);
        if (StringUtils.isBlank(binlogConfig.getHost())) {
            sb.append(NO_HOST_TIP);
        }
        if (StringUtils.isBlank(binlogConfig.getUsername())) {
            sb.append(NO_USERNAME_TIP);
        }
        String jdbcUrl = null;
        if (StringUtils.isBlank(binlogConfig.getHost())) {
            sb.append(NO_URL_TIP);
        } else {
            jdbcUrl = String.format(JDBC_URL_TEMPLATE, binlogConfig.getHost());
            //检测数据源连通性
            TelnetUtil.telnet(jdbcUrl);
        }

        if (sb.length() > 0) {
            throw new IllegalArgumentException(sb.toString());
        }
        //校验binlog cat
        if (StringUtils.isNotEmpty(binlogConfig.getCat())) {
            HashSet<String> set = Sets.newHashSet(DEFAULT_CAT.split(","));
            List<String> cats = Lists.newArrayList(binlogConfig.getCat().trim().toUpperCase().split(","));
            cats.removeIf(s -> set.contains(s.toUpperCase(Locale.ENGLISH)));
            if (CollectionUtils.isNotEmpty(cats)) {
                sb.append(String.format(BIN_LOG_TIP_TEMPLATE, cats.toString(), set.toString()));
            }
        }
        //校验binlog的start参数
        if (MapUtils.isNotEmpty(binlogConfig.getStart())) {
            try {
                MapUtils.getLong(binlogConfig.getStart(), TIMESTAMP);
            } catch (Exception e) {
                sb.append(String.format(BIN_LOG_START_TIMESTAMP_TIP_TEMPLATE, binlogConfig.getStart().get(TIMESTAMP)));
            }
            try {
                MapUtils.getLong(binlogConfig.getStart(), POSITION);
            } catch (Exception e) {
                sb.append(String.format(BIN_LOG_START_POSITION_TIP_TEMPLATE, binlogConfig.getStart().get(TIMESTAMP)));
            }
        }
        String finalJdbcUrl = jdbcUrl;
        try (Connection conn = RetryUtil.executeWithRetry(() -> DriverManager.getConnection(finalJdbcUrl, binlogConfig.getUsername(), binlogConfig.getPassword()), BinlogUtil.RETRY_TIMES, BinlogUtil.SLEEP_TIME, false)) {
            //校验用户权限
            if (!BinlogUtil.checkUserPrivilege(conn)) {
                sb.append(String.format(USER_PRIVILEGE_TIP_TEMPLATE, binlogConfig.getUsername()));
            }
            //校验数据库是否开启binlog
            if (!BinlogUtil.checkEnabledBinlog(conn)) {
                sb.append(BIN_LOG_NOT_ENABLED_TIP);
            }
            //校验数据库binlog_format是否设置为row
            if (!BinlogUtil.checkBinlogFormat(conn)) {
                sb.append(BIN_LOG_FORMAT_TIP);
            }
            //校验用户表是否有select权限
            Map<String, List<String>> filterDb = binlogConfig.getFilterDb();
            if (filterDb != null && !filterDb.isEmpty()) {
                Set<String> dataBases = filterDb.keySet();
                for (String dataBase : dataBases) {
                    List<String> failedTable = BinlogUtil.checkTablesPrivilege(conn, dataBase, binlogConfig.getFilter(), filterDb.get(dataBase));
                    if (CollectionUtils.isNotEmpty(failedTable)) {
                        sb.append(String.format(USER_SELECT_PRIVILEGE_TEMPLATE, JSONObject.toJSONString(failedTable)));
                    }
                    if (sb.length() > 0) {
                        throw new IllegalArgumentException(sb.toString());
                    }
                }
            }
        } catch (SQLException e) {
            StringBuilder detailsInfo = new StringBuilder(sb.length() + 128);
            if (sb.length() > 0) {
                detailsInfo.append(String.format(BIN_LOG_CONFIG_NOT_RIGHT_TIP_TEMPLATE, sb.toString()));
            }
            detailsInfo.append(String.format(ERROR_CHECK_BIN_LOG_TEMPLATE, e.getMessage()));
            throw new RuntimeException(detailsInfo.toString(), e);
        } catch (Exception e) {
            log.error("check binlog config error:{}", e.getMessage());
        }
    }

    /**
     * 处理指定binlog读取start位置
     */
    private EntryPosition findStartPosition() {
        EntryPosition startPosition = null;
        if (MapUtils.isNotEmpty(binlogConfig.getStart())) {
            startPosition = new EntryPosition();
            String journalName = (String) binlogConfig.getStart().get(JOURNAL_NAME);
            checkBinlogFile(journalName);

            if (StringUtils.isNotEmpty(journalName)) {
                startPosition.setJournalName(journalName);
            }
            startPosition.setTimestamp(MapUtils.getLong(binlogConfig.getStart(), TIMESTAMP));
            startPosition.setPosition(MapUtils.getLong(binlogConfig.getStart(), POSITION));
        }
        return startPosition;
    }

    private void checkBinlogFile(String journalName) {
        if (StringUtils.isNotEmpty(journalName)) {
            if (!new BinlogJournalValidator(binlogConfig.getHost(), binlogConfig.getPort(), binlogConfig.getUsername(), binlogConfig.getPassword()).check(journalName)) {
                throw new IllegalArgumentException("Can't find journalName: " + journalName);
            }
        }
    }


}
