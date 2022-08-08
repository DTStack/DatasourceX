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

package com.dtstack.dtcenter.common.loader.hbase.restful.cons;

/**
 * http请求url常量类
 *
 * @author luming
 * @date 2022/4/27
 */
public class HttpApi {
    /**
     * 获取hbase集群状态
     */
    public static final String GET_CLUSTER_STATUS = "%s/status/cluster";
    /**
     * 获取nameSpace下的所有表
     */
    public static final String GET_TABLES_BY_SCHEMA = "%s/namespaces/%s/tables";
    /**
     * 获取所有表
     */
    public static final String GET_TABLES = "%s";
    /**
     * 指定表获取列簇信息
     */
    public static final String GET_COLUMN_FAMILY = "%s/%s/schema";
    /**
     * 获取scanner
     */
    public static final String GET_SCANNER = "%s/%s/scanner";
    /**
     * 通过scannerId获取表数据
     */
    public static final String QUERY_BY_SCANNER = "%s/%s/scanner/%s";
    /**
     * 释放scanner资源
     */
    public static final String DELETE_SCANNER = "%s/%s/scanner/%s";
    /**
     * 获取所有的nameSpace
     */
    public static final String GET_NAMESPACES = "%s/namespaces";
}
