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

package com.dtstack.dtcenter.common.loader.starrocks;

import com.dtstack.dtcenter.common.loader.mysql5.MysqlTableClient;
import com.dtstack.dtcenter.common.loader.starrocks.util.DataSize;
import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Map;

/**
 * @author qiuyun
 * @version 1.0
 * @date 2022-07-22 21:43
 */
@Slf4j
public class StarRocksTableClient extends MysqlTableClient {
    /**
     * 获取表占用存储sql
     */
    private static final String TABLE_SIZE_SQL = "show data from `%s`.`%s`";

    protected static final String SHOW_PARTITIONS_SQL = "show partitions from `%s`";


    @Override
    protected String getTableSizeSql(String schema, String tableName) {
        if (StringUtils.isBlank(schema)) {
            throw new DtLoaderException("schema is not empty");
        }
        return String.format(TABLE_SIZE_SQL, schema, tableName);
    }

    @Override
    public Long getTableSize(ISourceDTO source, String schema, String tableName) {
        log.info("获取表占用存储，schema：{}，表名：{}", schema, tableName);
        if (StringUtils.isBlank(tableName)) {
            throw new DtLoaderException("Table name cannot be empty！");
        }
        String tableSizeSql = getTableSizeSql(schema, tableName);
        log.info("获取占用存储的sql：{}", tableSizeSql);
        List<Map<String, Object>> result = executeQuery(source, tableSizeSql);
        if (CollectionUtils.isEmpty(result) || MapUtils.isEmpty(result.get(0))) {
            throw new DtLoaderException("Obtaining Table Occupied Storage Information exception");
        }
        String totalSize = null;
        for (Map<String, Object> resultMap : result) {
            if ("Total".equals(MapUtils.getString(resultMap, "IndexName"))) {
                totalSize = MapUtils.getString(resultMap, "Size");
                break;
            }
        }
        if (StringUtils.isEmpty(totalSize)) {
            throw new DtLoaderException("Obtaining Table Occupied Storage Information exception:not Found");
        }
        totalSize = totalSize.replaceAll("\\s+", "");
        totalSize = totalSize.startsWith(".") ? "0" + totalSize : totalSize;
        return DataSize.parse(totalSize).toBytes();
    }

    @Override
    public List<String> showPartitions(ISourceDTO source, String tableName) {
        log.info("获取表所有分区，表名：{}", tableName);
        if (StringUtils.isBlank(tableName)) {
            throw new DtLoaderException("Table name cannot be empty！");
        }
        List<Map<String, Object>> result = executeQuery(source, String.format(SHOW_PARTITIONS_SQL, tableName));
        List<String> partitions = Lists.newArrayList();
        if (CollectionUtils.isNotEmpty(result)) {
            result.forEach(rs -> partitions.add(MapUtils.getString(rs, "PartitionName")));
        }
        return partitions;
    }

    /**
     * Alter Table 只支持三种操作类型:partition、rollup和schema change
     */
    @Override
    public Boolean alterTableParams(ISourceDTO source, String tableName, Map<String, String> params) {
        throw new DtLoaderException("Method not support");
    }
}
