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

import com.dtstack.dtcenter.loader.dto.tsdb.QueryResult;
import com.dtstack.dtcenter.loader.dto.tsdb.Suggest;
import com.dtstack.dtcenter.loader.dto.tsdb.TsdbPoint;
import com.dtstack.dtcenter.loader.dto.tsdb.TsdbQuery;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * TSDB interface
 *
 * @author ：wangchuan
 * date：Created in 下午2:24 2021/6/18
 * company: www.dtstack.com
 */
public interface TSDB extends Closeable {

    /**
     * 检查数据源连通性
     */
    void checkConnection();

    /**
     * close tsdb
     *
     * @param force 是否强制关闭 true or false
     * @throws IOException exception
     */
    void close(boolean force) throws IOException;

    /**
     * 同步插入多个点位
     *
     * @param points 插入点位
     * @return Result 返回结果
     */
    Boolean putSync(Collection<TsdbPoint> points);

    /**
     * 查询
     *
     * @param query 查询条件
     * @return result 查询结果
     */
    List<QueryResult> query(TsdbQuery query);

    /**
     * 根据 metric 、开始时间、结束时间删除数据
     *
     * @param metric    metric 名称
     * @param startTime 开始时间
     * @param endTime   结束时间
     */
    void deleteData(String metric, long startTime, long endTime);

    /**
     * 根据 metric 、tags、开始时间、结束时间删除数据
     *
     * @param metric    metric 名称
     * @param tags      tags 集合
     * @param startTime 开始时间
     * @param endTime   结束时间
     */
    void deleteData(String metric, Map<String, String> tags, long startTime, long endTime);

    /**
     * 根据 metric 、字段、开始时间、结束时间删除数据
     *
     * @param metric    metric 名称
     * @param fields    fields 集合
     * @param startTime 开始时间
     * @param endTime   结束时间
     */
    void deleteData(String metric, List<String> fields, long startTime, long endTime);

    /**
     * 根据 metric 、tags、字段、开始时间、结束时间删除数据
     *
     * @param metric    metric 名称
     * @param tags      tags 集合
     * @param fields    fields 结合
     * @param startTime 开始时间
     * @param endTime   结束时间
     */
    void deleteData(String metric, Map<String, String> tags, List<String> fields, long startTime, long endTime);

    /**
     * 根据 metric 、tags删除数据
     *
     * @param metric metric 名称
     * @param tags   tags 集合
     */
    void deleteMeta(String metric, Map<String, String> tags);

    /**
     * 根据 metric 、tags、fields删除数据
     *
     * @param metric metric 名称
     * @param tags   tags 集合
     * @param fields fields 集合
     */
    void deleteMeta(String metric, Map<String, String> tags, List<String> fields);

    /**
     * 根据 metric 、tags 删除数据
     *
     * @param metric     metric 名称
     * @param tags       tags 集合
     * @param deleteData 是否删除数据
     * @param recursive  递归删除
     */
    void deleteMeta(String metric, Map<String, String> tags, boolean deleteData, boolean recursive);

    /**
     * 根据 metric 、tags 删除数据
     *
     * @param metric     metric 名称
     * @param fields     fields 集合
     * @param tags       tags 集合
     * @param deleteData 是否删除数据
     * @param recursive  递归删除
     */
    void deleteMeta(String metric, List<String> fields, Map<String, String> tags, boolean deleteData, boolean recursive);

    /**
     * suggest method
     *
     * @param type   suggest 类型
     * @param prefix 前缀匹配
     * @param max    最大返回条数
     * @return result 返回结果
     */
    List<String> suggest(Suggest type, String prefix, int max);

    /**
     * suggest method
     *
     * @param type   suggest 类型
     * @param metric metric 名称
     * @param prefix 前缀匹配
     * @param max    最大返回条数
     * @return result 返回结果
     */
    List<String> suggest(Suggest type, String metric, String prefix, int max);

    /**
     * 获取版本号
     *
     * @return 版本号
     */
    String version();

    /**
     * 获取详细版本信息
     *
     * @return 详细版本信息
     */
    Map<String, String> getVersionInfo();
}
