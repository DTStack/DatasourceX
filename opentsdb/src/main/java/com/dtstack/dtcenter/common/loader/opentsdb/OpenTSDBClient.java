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

package com.dtstack.dtcenter.common.loader.opentsdb;

import com.dtstack.dtcenter.common.loader.common.nosql.AbsNoSqlClient;
import com.dtstack.dtcenter.common.loader.opentsdb.tsdb.OpenTSDBConnFactory;
import com.dtstack.dtcenter.common.loader.opentsdb.tsdb.TSDB;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;
import com.dtstack.dtcenter.loader.dto.tsdb.Aggregator;
import com.dtstack.dtcenter.loader.dto.tsdb.QueryResult;
import com.dtstack.dtcenter.loader.dto.tsdb.SubQuery;
import com.dtstack.dtcenter.loader.dto.tsdb.Suggest;
import com.dtstack.dtcenter.loader.dto.tsdb.TsdbQuery;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.google.common.collect.Lists;
import org.apache.commons.collections.CollectionUtils;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;

/**
 * OpenTSDB 客户端
 *
 * @author ：wangchuan
 * date：Created in 上午10:33 2021/6/17
 * company: www.dtstack.com
 */
public class OpenTSDBClient<T> extends AbsNoSqlClient<T> {

    @Override
    public Boolean testCon(ISourceDTO source) {
        try (TSDB openTSDBClient = OpenTSDBConnFactory.getOpenTSDBClient(source)) {
            openTSDBClient.checkConnection();
            return true;
        } catch (IOException e) {
            throw new DtLoaderException(e.getMessage(), e);
        }
    }

    @Override
    public List<String> getTableList(ISourceDTO source, SqlQueryDTO queryDTO) {
        return getTableListBySchema(source, queryDTO);
    }

    @Override
    public List<String> getTableListBySchema(ISourceDTO source, SqlQueryDTO queryDTO) {
        try (TSDB openTSDBClient = OpenTSDBConnFactory.getOpenTSDBClient(source)) {
            return openTSDBClient.suggest(Suggest.Metrics, queryDTO.getTableNamePattern(), queryDTO.getLimit());
        } catch (IOException e) {
            throw new DtLoaderException(e.getMessage(), e);
        }
    }

    @Override
    public List<List<Object>> getPreview(ISourceDTO source, SqlQueryDTO queryDTO) {
        // 默认预览 1 小时内，时间粒度 5 min 的数据，最多返回 20 个点位数据
        try (TSDB openTSDBClient = OpenTSDBConnFactory.getOpenTSDBClient(source)) {
            SubQuery subQuery = SubQuery.builder()
                    .metric(queryDTO.getTableName())
                    // 时间粒度 5 min
                    .downsample("5m-last")
                    .aggregator(Aggregator.LAST.getName())
                    .limit(queryDTO.getPreviewNum()).build();
            TsdbQuery query = TsdbQuery.builder()
                    .queries(Collections.singletonList(subQuery))
                    .start(System.currentTimeMillis() - 60 * 60 * 1000).build();
            List<QueryResult> queryResults = openTSDBClient.query(query);
            if (CollectionUtils.isEmpty(queryResults)) {
                return Collections.emptyList();
            }
            QueryResult queryResult = queryResults.get(0);
            LinkedHashMap<Long, Object> resultDps = queryResult.getDps();
            List<List<Object>> result = Lists.newArrayList();
            for (Long key : resultDps.keySet()) {
                if (result.size() >= queryDTO.getPreviewNum()) {
                    break;
                }
                List<Object> row = Lists.newArrayList();
                row.add(key);
                row.add(resultDps.get(key));
                result.add(row);
            }
            return result;
        } catch (IOException e) {
            throw new DtLoaderException(e.getMessage(), e);
        }
    }

    @Override
    public String getVersion(ISourceDTO source) {
        try (TSDB openTSDBClient = OpenTSDBConnFactory.getOpenTSDBClient(source)) {
            return openTSDBClient.version();
        } catch (Exception e) {
            throw new DtLoaderException(e.getMessage(), e);
        }
    }
}
