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

package com.dtstack.dtcenter.loader.cache.connection;

import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 13:52 2020/3/4
 * @Description：缓存连接信息
 */
@Slf4j
public class DataSourceConnection {
    /**
     * 默认过期时间
     */
    private static final int DEFUALT_VALIDITY_TIME = 60 * 5;

    /**
     * 连接主键，标志唯一性
     */
    private String sessionKey;

    /**
     * 过期时间戳
     */
    private Long timeoutStamp;

    /**
     * 数据源节点信息
     */
    private List<DataSourceCacheNode> sourceNodes = Collections.synchronizedList(new ArrayList<>());

    public DataSourceConnection() {
        refreshTimeoutStamp();
    }

    /**
     * 更新 连接主键
     *
     * @param sessionKey
     */
    public void updateSessionKey(String sessionKey) {
        if (StringUtils.isBlank(sessionKey)) {
            throw new DtLoaderException("The data source connection primary key (sessionKey) cannot be empty");
        }

        this.sessionKey = sessionKey;
        refreshTimeoutStamp();
    }

    /**
     * 获取资源节点
     *
     * @param sourceType
     * @return
     */
    public DataSourceCacheNode getSourceCacheNode(Integer sourceType) {
        refreshTimeoutStamp();
        for (DataSourceCacheNode node : sourceNodes) {
            if (!node.getSourceType().equals(sourceType)) {
                continue;
            }
            return node;
        }

        return null;
    }

    /**
     * 新增数据源节点
     *
     * @param sourceType
     * @param connection
     */
    public void addNode(Integer sourceType, Connection connection) {
        if (connection == null || sourceType == null) {
            return;
        }

        DataSourceCacheNode node = getSourceCacheNode(sourceType);
        if (node != null) {
            node.close();
            this.sourceNodes.remove(node);
        }

        DataSourceCacheNode cacheNode =
                DataSourceCacheNode.builder().sourceType(sourceType).connection(connection).build();
        this.sourceNodes.add(cacheNode);
    }

    /**
     * 根据数据源类型获取连接
     *
     * @param sourceType
     * @return
     */
    public Connection getConnection(Integer sourceType) {
        DataSourceCacheNode cacheNode = getSourceCacheNode(sourceType);
        return cacheNode == null ? null : cacheNode.getConnection();
    }


    /**
     * 获取 连接主键
     *
     * @return
     */
    public String getSessionKey() {
        return this.sessionKey;
    }

    public Integer getConSize() {
        return this.sourceNodes.size();
    }

    /**
     * 关闭之前存在的数据库连接
     */
    public void close() {
        log.info("close connection SessionKey = {}", this.getSessionKey());
        sourceNodes.forEach(node -> node.close());
        sourceNodes.clear();
    }

    public void close(Integer sourceType) {
        log.info("close connection SessionKey = {}", this.getSessionKey());

        for (int i = 0; i < sourceNodes.size(); i++) {
            if (!sourceNodes.get(i).getSourceType().equals(sourceType)) {
                continue;
            }
            sourceNodes.get(i).close();
            sourceNodes.remove(sourceNodes.get(i));
            break;
        }
    }

    public Long getTimeoutStamp() {
        return timeoutStamp;
    }

    /**
     * 刷新有效时间
     */
    private void refreshTimeoutStamp() {
        this.timeoutStamp = System.currentTimeMillis() + DEFUALT_VALIDITY_TIME * 1000;
    }
}
