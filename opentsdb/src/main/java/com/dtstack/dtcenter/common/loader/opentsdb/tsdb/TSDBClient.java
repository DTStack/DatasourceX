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

package com.dtstack.dtcenter.common.loader.opentsdb.tsdb;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.dtstack.dtcenter.common.loader.opentsdb.http.HttpAPI;
import com.dtstack.dtcenter.common.loader.opentsdb.http.HttpClient;
import com.dtstack.dtcenter.common.loader.opentsdb.http.HttpClientFactory;
import com.dtstack.dtcenter.common.loader.opentsdb.http.response.HttpStatus;
import com.dtstack.dtcenter.common.loader.opentsdb.http.response.ResultResponse;
import com.dtstack.dtcenter.common.loader.opentsdb.query.DeleteMetaRequest;
import com.dtstack.dtcenter.common.loader.opentsdb.query.MetricTimeRange;
import com.dtstack.dtcenter.common.loader.opentsdb.query.SuggestValue;
import com.dtstack.dtcenter.common.loader.opentsdb.query.Timeline;
import com.dtstack.dtcenter.loader.dto.source.OpenTSDBSourceDTO;
import com.dtstack.dtcenter.loader.dto.tsdb.QueryResult;
import com.dtstack.dtcenter.loader.dto.tsdb.Suggest;
import com.dtstack.dtcenter.loader.dto.tsdb.TsdbPoint;
import com.dtstack.dtcenter.loader.dto.tsdb.TsdbQuery;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.google.common.collect.Maps;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpResponse;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * TSDB Client
 *
 * @author ：wangchuan
 * date：Created in 下午2:23 2021/6/18
 * company: www.dtstack.com
 */
@Slf4j
public class TSDBClient implements TSDB {

    protected final HttpClient httpclient;

    private final String url;

    private static final String EMPTY_HOLDER = new JSONObject().toJSONString();

    public TSDBClient(OpenTSDBSourceDTO sourceDTO) {
        this.url = sourceDTO.getUrl();
        this.httpclient = HttpClientFactory.createHttpClient(sourceDTO);
        this.httpclient.start();
        log.info("The tsdb client has started. url:{}", sourceDTO.getUrl());
    }

    /**
     * 检查连通性
     */
    @Override
    public void checkConnection() {
        try {
            httpclient.post(HttpAPI.VIP_HEALTH, EMPTY_HOLDER);
        } catch (Exception e) {
            throw new DtLoaderException(String.format("open tsdb Connection failed,url:%s", url));
        }
    }

    @Override
    public Boolean putSync(Collection<TsdbPoint> points) {
        String jsonBody = JSON.toJSONString(points, SerializerFeature.DisableCircularReferenceDetect);
        HttpResponse httpResponse = httpclient.post(HttpAPI.PUT, jsonBody);
        ResultResponse resultResponse = ResultResponse.simplify(httpResponse);
        handleStatus(resultResponse);
        return true;
    }

    @Override
    public List<QueryResult> query(TsdbQuery query) {
        String jsonBody = JSON.toJSONString(query, SerializerFeature.DisableCircularReferenceDetect);
        HttpResponse httpResponse = httpclient.post(HttpAPI.QUERY, jsonBody);
        ResultResponse resultResponse = ResultResponse.simplify(httpResponse);
        handleStatus(resultResponse);
        String content = resultResponse.getContent();
        List<QueryResult> queryResultList = JSON.parseArray(content, QueryResult.class);
        setTypeIfNeeded(query, queryResultList);
        return queryResultList;
    }

    @Override
    public void deleteData(String metric, long startTime, long endTime) {
        MetricTimeRange metricTimeRange = new MetricTimeRange(metric, startTime, endTime);
        HttpResponse httpResponse = httpclient.post(HttpAPI.DELETE_DATA, metricTimeRange.toJSON());
        ResultResponse resultResponse = ResultResponse.simplify(httpResponse);
        handleStatus(resultResponse);
    }

    @Override
    public void deleteData(String metric, Map<String, String> tags, long startTime, long endTime) {
        MetricTimeRange metricTimeRange = new MetricTimeRange(metric, tags, startTime, endTime);
        HttpResponse httpResponse = httpclient.post(HttpAPI.DELETE_DATA, metricTimeRange.toJSON());
        ResultResponse resultResponse = ResultResponse.simplify(httpResponse);
        handleStatus(resultResponse);
    }

    @Override
    public void deleteData(String metric, List<String> fields, long startTime, long endTime) {
        MetricTimeRange metricTimeRange = new MetricTimeRange(metric, fields, startTime, endTime);
        HttpResponse httpResponse = httpclient.post(HttpAPI.DELETE_DATA, metricTimeRange.toJSON());
        ResultResponse resultResponse = ResultResponse.simplify(httpResponse);
        handleStatus(resultResponse);
    }

    @Override
    public void deleteData(String metric, Map<String, String> tags, List<String> fields, long startTime, long endTime) {
        MetricTimeRange metricTimeRange = new MetricTimeRange(metric, tags, fields, startTime, endTime);
        HttpResponse httpResponse = httpclient.post(HttpAPI.DELETE_DATA, metricTimeRange.toJSON());
        ResultResponse resultResponse = ResultResponse.simplify(httpResponse);
        handleStatus(resultResponse);
    }

    @Override
    public void deleteMeta(String metric, Map<String, String> tags) {
        deleteMeta(new Timeline(metric, tags));
    }

    @Override
    public void deleteMeta(String metric, Map<String, String> tags, List<String> fields) {
        deleteMeta(new Timeline(metric, tags, fields));
    }

    @Override
    public void deleteMeta(String metric, Map<String, String> tags, boolean deleteData, boolean recursive) {
        deleteMeta(new DeleteMetaRequest(metric, tags, deleteData, recursive));
    }

    @Override
    public void deleteMeta(String metric, List<String> fields, Map<String, String> tags, boolean deleteData, boolean recursive) {
        deleteMeta(new DeleteMetaRequest(metric, tags, fields, deleteData, recursive));
    }

    @Override
    public List<String> suggest(Suggest type, String prefix, int max) {
        SuggestValue suggestValue = new SuggestValue(type.getName(), prefix, max);
        return suggest(suggestValue);
    }

    @Override
    public List<String> suggest(Suggest type, String metric, String prefix, int max) {
        SuggestValue suggestValue = new SuggestValue(type.getName(), metric, prefix, max);
        return suggest(suggestValue);
    }

    @Override
    public String version() {
        HttpResponse httpResponse = httpclient.post(HttpAPI.VERSION, EMPTY_HOLDER);
        ResultResponse resultResponse = ResultResponse.simplify(httpResponse);
        handleStatus(resultResponse);
        JSONObject result = JSONObject.parseObject(resultResponse.getContent());
        return result.getString("version");
    }

    @Override
    public Map<String, String> getVersionInfo() {
        HttpResponse httpResponse = httpclient.post(HttpAPI.VERSION, EMPTY_HOLDER);
        ResultResponse resultResponse = ResultResponse.simplify(httpResponse);
        handleStatus(resultResponse);
        JSONObject result = JSONObject.parseObject(resultResponse.getContent());
        Map<String, String> versionInfo = Maps.newHashMap();
        for (String key : result.keySet()) {
            versionInfo.put(key, result.getString(key));
        }
        return versionInfo;
    }

    private List<String> suggest(SuggestValue suggestValue) {
        HttpResponse httpResponse = httpclient.post(HttpAPI.SUGGEST, suggestValue.toJSON());
        ResultResponse resultResponse = ResultResponse.simplify(httpResponse);
        handleStatus(resultResponse);
        String content = resultResponse.getContent();
        return JSON.parseArray(content, String.class);
    }

    private void deleteMeta(Timeline timeline) {
        HttpResponse httpResponse = httpclient.post(HttpAPI.DELETE_META, timeline.toJSON());
        ResultResponse resultResponse = ResultResponse.simplify(httpResponse);
        handleStatus(resultResponse);
    }

    public void deleteMeta(DeleteMetaRequest request) {
        HttpResponse httpResponse = httpclient.post(HttpAPI.DELETE_META, request.toJSON());
        ResultResponse resultResponse = ResultResponse.simplify(httpResponse);
        handleStatus(resultResponse);
    }

    public static void setTypeIfNeeded(TsdbQuery query, final List<QueryResult> queryResultList) {
        if (query == null || !query.isShowType()) {
            return;
        }
        final Class<?> showType = query.getType();
        for (QueryResult queryResult : queryResultList) {
            if (showType != null) {
                queryResult.setType(showType);
                continue;
            }
            final LinkedHashMap<Long, Object> dps = queryResult.getDps();
            if (dps == null || dps.size() == 0) {
                continue;
            }
            // long 和 double 共存情况下返回 BigDecimal 类型
            Class<?> typeExist = null;
            for (Map.Entry<Long, Object> entry : dps.entrySet()) {
                final Object value = entry.getValue();
                final Class<?> type = getType4Single(value);
                if (type == BigDecimal.class) {
                    typeExist = BigDecimal.class;
                    break;
                }
                if (typeExist == null) {
                    typeExist = type;
                    continue;
                }
                if (typeExist != type) {
                    typeExist = BigDecimal.class;
                    break;
                }
            }
            queryResult.setType(typeExist);
        }
    }

    public static Class<?> getType4Single(Object value) {
        if (value instanceof Byte || value instanceof Short || value instanceof Integer || value instanceof Long) {
            return Long.class;
        } else if (value instanceof Float) {
            return Double.class;
        } else if (value instanceof Double) {
            return Double.class;
        } else return getOtherClass(value);
    }

    private static Class<?> getOtherClass(Object value) {
        if (value instanceof BigDecimal) {
            return BigDecimal.class;
        } else if (value instanceof Boolean) {
            return Boolean.class;
        } else if (value instanceof String) {
            return String.class;
        } else {
            // 未考虑到的数据类型抛出 Object
            log.warn("There is a data type that has not been considered, detail: " + value);
            return Object.class;
        }
    }

    /**
     * 处理返回值状态码
     *
     * @param resultResponse 返回值
     */
    protected void handleStatus(ResultResponse resultResponse) {
        HttpStatus httpStatus = resultResponse.getHttpStatus();
        switch (httpStatus) {
            case ServerNotSupport:
                throw new DtLoaderException("Server not support");
            case ServerError:
                throw new DtLoaderException("Server error");
            case ServerUnauthorized:
                throw new DtLoaderException("Server unauthorized");
            case ServerSuccessNoContent:
            case ServerSuccess:
                return;
            default:
                throw new DtLoaderException("Server status unknow");
        }
    }

    @Override
    public void close() throws IOException {
        close(false);
    }

    @Override
    public void close(boolean force) throws IOException {
        httpclient.close(force);
        log.info("The tsdb client has closed.");
    }
}
