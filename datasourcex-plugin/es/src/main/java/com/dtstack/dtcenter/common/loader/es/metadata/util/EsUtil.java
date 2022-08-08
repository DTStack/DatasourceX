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

package com.dtstack.dtcenter.common.loader.es.metadata.util;

import com.alibaba.fastjson.JSON;
import com.dtstack.dtcenter.common.loader.es.metadata.cons.MetaDataEsCons;
import com.dtstack.dtcenter.common.loader.es.metadata.entity.AliasEntity;
import com.dtstack.dtcenter.common.loader.es.metadata.entity.ColumnEntity;
import com.dtstack.dtcenter.common.loader.es.metadata.entity.FieldEntity;
import com.dtstack.dtcenter.common.loader.es.metadata.entity.IndexProperties;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;

import java.io.IOException;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static com.dtstack.dtcenter.common.loader.es.metadata.cons.MetaDataEsCons.MAP_PROPERTIES;

/**
 * es操作工具类
 *
 * @author : luming
 * @date 2022年04月13日
 */
public class EsUtil {

    private static final NumberFormat nf = NumberFormat.getInstance();

    static {
        nf.setGroupingUsed(false);
    }

    /**
     * 返回指定索引的配置信息
     *
     * @param indexName  索引名称
     * @param restClient ES6 LowLevelRestClient
     * @return 索引的配置信息
     * @throws IOException
     */
    @SuppressWarnings("unchecked")
    public static IndexProperties queryIndexProp(String indexName, RestClient restClient) {
        IndexProperties indexProperties = new IndexProperties();
        /*
         * 字符串数组indexByCat的数据结构如下，数字下标对于具体变量位置
         * 0      1      2        3                      4   5   6          7            8          9
         * health status index    uuid                   pri rep docs.count docs.deleted store.size pri.store.size
         * yellow open   megacorp LYXJZVslTaiTOtQzVFqfLg   5   1          0            0      1.2kb          1.2kb
         */
        String[] indexByCat = queryIndexByCat(indexName, restClient);

        indexProperties.setHealth(indexByCat[0]);
        indexProperties.setStatus(indexByCat[1]);
        indexProperties.setUuid(indexByCat[3]);
        indexProperties.setCount(indexByCat[6]);
        indexProperties.setDocsDeleted(indexByCat[7]);
        indexProperties.setTotalSize(parseToNumericInByte(indexByCat[8]));
        indexProperties.setPriSize(parseToNumericInByte(indexByCat[9]));

        Map<String, Object> index = queryIndex(indexName, restClient);
        Map<String, Object> settings = getMap(getMap(index, indexName), MetaDataEsCons.MAP_SETTINGS);
        Map<String, String> indexSetting = (Map<String, String>) settings.get(MetaDataEsCons.KEY_INDEX);

        indexProperties.setCreateTime(indexSetting.get(MetaDataEsCons.MAP_CREATION_DATE));
        indexProperties.setShards(indexSetting.get(MetaDataEsCons.MAP_NUMBER_OF_SHARDS));
        indexProperties.setReplicas(indexSetting.get(MetaDataEsCons.MAP_NUMBER_OF_REPLICAS));

        return indexProperties;
    }

    /**
     * 将1.2kb / 500.6mb等带单位的字符串转换成byte单位的数字字符串
     *
     * @param origin origin string like 500.6mb
     * @return
     */
    private static String parseToNumericInByte(String origin) {

        if (StringUtils.isBlank(origin)) {
            return null;
        }

        char[] chars = origin.toCharArray();
        int lastDigit = 0, current = 0;
        for (char c : chars) {
            if (Character.isDigit(c)) {
                lastDigit = current;
            }
            current++;
        }
        if (lastDigit == chars.length - 1) return origin;
        String numeric = origin.substring(0, lastDigit + 1);
        String unit = origin.substring(lastDigit + 1, chars.length);
        double doubleV = Double.parseDouble(numeric);
        switch (unit) {
            case "kb":
                return nf.format(doubleV * 1024);
            case "mb":
                return nf.format(doubleV * (1024 * 1024L));
            case "gb":
                return nf.format(doubleV * (1024 * 1024 * 1024L));
            case "tb":
                return nf.format(doubleV * (1024 * 1024 * 1024 * 1024L));
            default:
                return numeric;
        }
    }

    /**
     * 查询指定索引下的所有字段信息
     *
     * @param indexName  索引名称
     * @param restClient LowLevelRestClient
     * @return 字段信息
     */
    public static List<ColumnEntity> queryColumns(String indexName, RestClient restClient) {
        Map<String, Object> index = queryIndex(indexName, restClient);
        Map<String, Object> mappings = getMap(getMap(index, indexName), MetaDataEsCons.MAP_MAPPINGS);
        if (mappings.isEmpty()) {
            return new ArrayList<>();
        }

        for (String key : mappings.keySet()) {
            if (MAP_PROPERTIES.equals(key)) {
                Map<String, Object> propertiesMap = getMap(mappings, MAP_PROPERTIES);
                return getColumn(propertiesMap, "", new ArrayList<>(16));
            } else {
                Object value = MapUtils.getObject(mappings, key);
                if (value instanceof Map) {
                    // es5.x has index type.
                    Map<String, Object> typeMap = getMap(mappings, key);
                    if (typeMap.containsKey(MAP_PROPERTIES)) {
                        Map<String, Object> propertiesMap = getMap(typeMap, MAP_PROPERTIES);
                        return getColumn(propertiesMap, "", new ArrayList<>(16));
                    }
                }
            }
        }

        return new ArrayList<>();
    }

    /**
     * 返回字段列表
     *
     * @param docs       未经处理包含所有字段信息的map
     * @param columnName 字段名
     * @param columnList 处理后的字段列表
     * @return 字段列表
     */
    public static List<ColumnEntity> getColumn(Map<String, Object> docs, String columnName, List<ColumnEntity> columnList) {
        // docs 为 properties 中所有子元素, properties 中第一级不可能为 properties
        for (String key : docs.keySet()) {
            // 第一级下子元素一定为对象, key 为字段名, value 为字段说明
            Map<String, Object> columns = getMap(docs, key);
            String columnNameTmp;
            if (StringUtils.isNotBlank(columnName)) {
                columnNameTmp = columnName + MetaDataEsCons.POINT_SYMBOL + key;
            } else {
                columnNameTmp = key;
            }
            // 包含 type 字段则保存当前字段到 list 中
            if (columns.containsKey(MetaDataEsCons.MAP_TYPE)) {
                String type = MapUtils.getString(columns, MetaDataEsCons.MAP_TYPE);
                ColumnEntity column = new ColumnEntity();
                column.setName(columnNameTmp);
                column.setType(type);
                if (columns.get(MetaDataEsCons.KEY_FIELDS) != null) {
                    column.setFieldList(getFieldList(columns));
                }
                int columnIndex = columnList.size() + 1;
                column.setIndex(columnIndex);
                columnList.add(column);
            }
            // 表示为嵌套索引 nested, 继续递归
            if (columns.containsKey(MetaDataEsCons.MAP_PROPERTIES)) {
                getColumn(getMap(columns, MAP_PROPERTIES), columnNameTmp, columnList);
            }
        }
        return columnList;
    }

    /**
     * 返回字段映射参数
     *
     * @param docs 该字段属性map
     * @return 字段映射参数
     */
    @SuppressWarnings("unchecked")
    public static List<FieldEntity> getFieldList(Map<String, Object> docs) {
        Map<String, Map<String, String>> fields = (Map<String, Map<String, String>>) docs.get(MetaDataEsCons.KEY_FIELDS);
        List<FieldEntity> fieldsList = new ArrayList<>(16);
        FieldEntity field = new FieldEntity();
        for (String key : fields.keySet()) {
            field.setFieldName(key);
            field.setFieldProp(fields.get(key).toString());
            fieldsList.add(field);
        }
        return fieldsList;
    }

    /**
     * 查询索引别名
     *
     * @param indexName  索引名称
     * @param restClient ES6 LowLevelRestClient
     * @return 索引别名
     */
    public static List<AliasEntity> queryAliases(String indexName, RestClient restClient) {
        List<AliasEntity> aliasList = new ArrayList<>();
        AliasEntity alias = new AliasEntity();
        Map<String, Object> index = queryIndex(indexName, restClient);
        Map<String, Object> aliases = getMap(getMap(index, indexName), MetaDataEsCons.MAP_ALIASES);
        for (String key : aliases.keySet()) {
            alias.setAliasName(key);
            alias.setAliasProp(aliases.get(key).toString());
            aliasList.add(alias);
        }
        return aliasList;
    }

    /**
     * 使用/_cat/indices{index}的方式查询指定index
     *
     * @param restClient ES6 LowLevelRestClient
     * @param indexName  索引名称
     * @return index
     */
    public static String[] queryIndexByCat(String indexName, RestClient restClient) {
        Map<String, String> params = Collections.singletonMap(MetaDataEsCons.KEY_INDEX, indexName);
        String resBody;
        try {
            Response response = restClient.performRequest(MetaDataEsCons.API_METHOD_GET, MetaDataEsCons.API_ENDPOINT_CAT_INDEX, params);
            resBody = EntityUtils.toString(response.getEntity());
        } catch (IOException e) {
            throw new DtLoaderException("query es error", e);
        }
        return resBody.split("\\s+");
    }

    /**
     * indexName为*表示查询所有的索引信息
     *
     * @param restClient ES6 LowLevelRestClient
     * @return
     */
    public static String[] queryIndicesByCat(RestClient restClient) {
        return queryIndexByCat(MetaDataEsCons.STAR_SYMBOL, restClient);
    }

    /**
     * 使用/index的方式查询指定索引的详细信息
     *
     * @param indexName  索引名称
     * @param restClient ES6 LowLevelRestClient
     * @return
     */
    @SuppressWarnings("unchecked")
    public static Map<String, Object> queryIndex(String indexName, RestClient restClient) {
        String endpoint = MetaDataEsCons.SINGLE_SLASH_SYMBOL + indexName;
        String resBody;
        try {
            Response response = restClient.performRequest(MetaDataEsCons.API_METHOD_GET, endpoint);
            resBody = EntityUtils.toString(response.getEntity());
        } catch (IOException e) {
            throw new DtLoaderException("query es error", e);
        }
        return (Map<String, Object>) JSON.parseObject(resBody, Map.class);
    }

    /**
     * 将map中key对应的value值转换成Map<String, Object>并返回
     *
     * @param map
     * @param key
     * @return 强转后的value值
     */
    @SuppressWarnings("unchecked")
    public static Map<String, Object> getMap(Map<String, Object> map, String key) {
        try {
            return (Map<String, Object>) map.get(key);
        } catch (Exception e) {
            throw new RuntimeException(String.format("Map类型强转失败, key: %s, map: %s", key, map), e);
        }
    }
}
