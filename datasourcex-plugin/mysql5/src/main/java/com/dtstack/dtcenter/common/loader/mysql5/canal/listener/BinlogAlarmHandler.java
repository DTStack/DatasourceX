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

import com.alibaba.otter.canal.common.AbstractCanalLifeCycle;
import com.alibaba.otter.canal.common.alarm.CanalAlarmHandler;
import com.alibaba.otter.canal.parse.inbound.mysql.MysqlEventParser;
import lombok.extern.slf4j.Slf4j;

/**
 * 基于log的告警处理器
 *
 * @author zhiyi
 */
@Slf4j
public class BinlogAlarmHandler extends AbstractCanalLifeCycle implements CanalAlarmHandler {

    private static final String BINARY_LOG_LOST = "Could not find first log file name in binary log index file";

    /**
     * 程序是否需要终止
     */
    private boolean exit = false;

    private MysqlEventParser eventParser;

    public BinlogAlarmHandler() {
    }

    @Override
    public void sendAlarm(String destination, String msg) {
        log.error("destination:{}[{}]", destination, msg);
        if (msg.contains(BINARY_LOG_LOST)) {
            eventParser.stop();
            if (exit) {
                System.exit(-1);
            }
        }
    }

    public void setExit(boolean exit) {
        this.exit = exit;
    }

    public void setEventParser(MysqlEventParser eventParser) {
        this.eventParser = eventParser;
    }
}
