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

package com.dtstack.dtcenter.loader.dto.canal;


import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * binlog 配置
 *
 * @author sean
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class BinlogConfigDTO implements Serializable {

    /**
     * 监听的msyql主机ip
     */
    private String host;

    /**
     * 监听的msyql主机端口
     */
    private Integer port = 3306;

    /**
     * mysql主机连接用户名
     */
    private String username;

    /**
     * mysql主机连接密码
     */
    private String password;

    /**
     * 监听的数据库及表（key: schema value: table）
     */
    private Map<String, List<String>> filterDb;

    /**
     * 要读取的binlog文件的开始位置
     */
    private Map<String, Object> start;

    /**
     * 监听的事件类型，英文逗号分割
     */
    private String cat;

    /**
     * 监听的库表表达式
     */
    private String filter;

    /**
     * 并发缓存大小
     */
    private Integer bufferSize = 256;

    private Integer transactionSize = 1024;

    /**
     * 从服务器的ID
     */
    private Long slaveId = (long) (new Object().hashCode());

    /**
     * 连接编码
     */
    private String connectionCharset = "UTF-8";

    /**
     * 是否开启心跳检测
     */
    private Boolean detectingEnable = true;

    /**
     * 心跳sql
     */
    private String detectingSql = "SELECT CURRENT_DATE";

    /**
     * 是否开启时序表结构能力
     */
    private Boolean enableTsdb = true;

    /**
     * 是否开启并行解析binlog日志
     */
    private Boolean parallel = true;

    /**
     * 并行解析binlog日志线程数
     */
    private Integer parallelThreadSize = 2;

    /**
     * 是否开启gtid模式
     */
    private Boolean isGtidMode = false;

    /**
     * 发生异常程序是否需要终止
     */
    private Boolean exit = false;

    /**
     * 自定义参数信息
     */
    private Map<Object, Object> otherParams;
}
