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

package com.dtstack.dtcenter.common.loader.doris.metadata.util;

import com.alibaba.fastjson.JSON;
import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Json工具类
 *
 * @author luming
 * @date 2022/4/11
 */
@Slf4j
public class JsonUtils {
    /**
     * 将查询结果Result Set 转为定义的Java Bean
     *
     * @param resultSet 查询结果集
     * @param clazz     转化的Java Bean Class
     * @return Java Bean 对象
     */
    public static <T> T toJavaBean(ResultSet resultSet, Class<T> clazz) {
        try {
            List<String> columnNameList = getColumnName(resultSet);
            Map<String, Object> columnMap = new HashMap<>();
            if (resultSet.next()) {
                for (String columnName : columnNameList) {
                    //doris check_time参数解析有问题，只有test库有问题
                    if ("CHECK_TIME".equals(columnName)){
                        continue;
                    }
                    columnMap.put(columnName, resultSet.getObject(columnName));
                }
                String toJson = JSON.toJSONString(columnMap);
                return JSON.parseObject(toJson, clazz);
            }
            log.warn("Get empty result. Return empty instance of {}", clazz.getName());
            return clazz.newInstance();
        } catch (Exception e) {
            throw new RuntimeException(
                    String.format("ResultSet to Java bean [%s] failed.", clazz), e);
        }
    }

    /**
     * 将查询结果Result Set 转为定义的Java Bean 队列。
     *
     * @param resultSet 查询结果集
     * @param clazz     转化的Java Bean Class
     * @return Java Bean 对象队列
     */
    public static <T> List<T> toJavaBeanList(ResultSet resultSet, Class<T> clazz) {
        try {
            List<T> objects = Lists.newArrayList();
            List<String> columnNameList = getColumnName(resultSet);
            while (resultSet.next()) {
                Map<String, Object> columnMap = new HashMap<>();
                for (String columnName : columnNameList) {
                    columnMap.put(columnName, resultSet.getObject(columnName));
                }
                String toJson = JSON.toJSONString(columnMap);
                objects.add(JSON.parseObject(toJson, clazz));
            }
            return objects;
        } catch (Exception e) {
            throw new RuntimeException(
                    String.format("ResultSet to List of Java bean [%s] failed.", clazz), e);
        }
    }

    /**
     * 从查询结果集中返回表字段名
     *
     * @param resultSet 查询结果集
     * @return 字段名
     */
    private static List<String> getColumnName(ResultSet resultSet) {
        List<String> metadataList = new ArrayList<>();
        try {
            ResultSetMetaData metaData = resultSet.getMetaData();
            int columnCount = metaData.getColumnCount();
            for (int index = 0; index < columnCount; index++) {
                metadataList.add(metaData.getColumnName(index + 1));
            }
            return metadataList;
        } catch (SQLException e) {
            throw new RuntimeException("Get metadata from result failed.", e);
        }
    }
}
