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
import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 16:27 2020/3/4
 * @Description：数据源具体节点信息
 */
@Data
@Builder
@Slf4j
public class DataSourceCacheNode {
    /**
     * 数据源类型
     */
    private Integer sourceType;

    /**
     * 连接信息
     */
    private Connection connection;

    /**
     * 关闭数据库连接
     */
    public void close() {
        if (connection != null) {
            try {
                log.info("close connection sourceType = {}", sourceType);
                connection.close();
            } catch (SQLException e) {
                throw new DtLoaderException(String.format("cache connection closed failed：%s", e.getMessage()), e);
            }
        }
    }
}
