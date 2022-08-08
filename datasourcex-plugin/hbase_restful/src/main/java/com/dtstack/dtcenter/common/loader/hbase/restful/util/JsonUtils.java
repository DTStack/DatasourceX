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

package com.dtstack.dtcenter.common.loader.hbase.restful.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.dtstack.dtcenter.common.loader.common.utils.HexUtil;
import com.dtstack.dtcenter.common.loader.hbase.restful.cons.NormalCons;
import com.dtstack.dtcenter.loader.dto.ColumnMetaDTO;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * json工具类
 *
 * @author luming
 * @date 2022/5/9
 */
public class JsonUtils {
    /**
     * 解析content的tableList
     *
     * @param content http content
     * @return tables
     */
    public static List<String> parseTables(String content) {
        return Optional.ofNullable(content)
                .map(JSONObject::parseObject)
                .map(json -> json.getJSONArray("table"))
                .map(tables -> {
                    List<String> tableList = new ArrayList<>();
                    for (int i = 0; i < tables.size(); i++) {
                        tableList.add(tables.getJSONObject(i).getString("name"));
                    }
                    return tableList;
                })
                .orElse(Lists.newArrayList());
    }

    /**
     * 解析content的columns
     *
     * @param content http content
     * @return columns
     */
    public static List<ColumnMetaDTO> parseColumns(String content) {
        return Optional.ofNullable(content)
                .map(JSONObject::parseObject)
                .map(ct -> ct.getJSONArray("ColumnSchema"))
                .map(cfs -> {
                    List<ColumnMetaDTO> cmList = new ArrayList<>();
                    for (int i = 0; i < cfs.size(); i++) {
                        String key = cfs.getJSONObject(i).getString("name");
                        ColumnMetaDTO cmDTO = new ColumnMetaDTO();
                        cmDTO.setKey(key);
                        cmList.add(cmDTO);
                    }
                    return cmList;
                })
                .orElse(Lists.newArrayList());
    }

    /**
     * 解析content的namespaces
     *
     * @param content http content
     * @return namespaces
     */
    public static List<String> parseNamespaces(String content) {
        return Optional.ofNullable(content)
                .map(JSONObject::parseObject)
                .map(jsonObj -> jsonObj.getJSONArray("Namespace"))
                .map(jsonArr -> jsonArr.toJavaList(String.class))
                .orElse(Lists.newArrayList());
    }

    /**
     * 解析scan请求的结果
     *
     * @param content scan请求后未解析的内容
     * @return result
     */
    public static List<Map<String, Object>> parseScanResult(String content) {
        List<Map<String, Object>> results = Lists.newArrayList();

        JSONArray rowKeys = JSON.parseObject(content).getJSONArray("Row");
        for (int i = 0; i < rowKeys.size(); i++) {
            JSONObject rkObj = rowKeys.getJSONObject(i);
            String rowKey = rkObj.getString("key");

            JSONArray cells = rkObj.getJSONArray("Cell");
            for (int j = 0; j < cells.size(); j++) {
                JSONObject cell = cells.getJSONObject(j);
                long timestamp = cell.getLongValue("timestamp");
                String familyQualifier = cell.getString("column");
                String value = cell.getString("$");

                Map<String, Object> row = Maps.newHashMap();
                row.put(NormalCons.ROW_KEY, HexUtil.base64De(rowKey));
                row.put(NormalCons.TIMESTAMP, timestamp);
                row.put(HexUtil.base64De(familyQualifier), HexUtil.base64De(value));
                results.add(row);
            }
        }

        return results;
    }
}
