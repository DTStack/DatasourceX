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

package com.dtstack.dtcenter.common.loader.metadata.collect;

import com.alibaba.fastjson.JSON;
import com.dtstack.dtcenter.common.loader.metadata.constants.BaseCons;
import com.dtstack.dtcenter.common.loader.metadata.core.MetadataBaseCollectSplit;
import com.dtstack.dtcenter.loader.dto.metadata.MetadataCollectCondition;
import com.dtstack.dtcenter.loader.dto.metadata.entity.MetadataEntity;
import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;
import com.dtstack.dtcenter.loader.utils.AssertUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * abs metadata collect
 *
 * @author ：wangchuan
 * date：Created in 下午6:02 2022/4/6
 * company: www.dtstack.com
 */
@Slf4j
public abstract class AbstractMetaDataCollect implements MetaDataCollect {

    public static final AtomicBoolean IS_INIT = new AtomicBoolean(false);

    /**
     * 数据源信息
     */
    protected ISourceDTO sourceDTO;

    /**
     * 元数据采集条件
     */
    protected MetadataCollectCondition metadataCollectCondition;

    /**
     * 元数据采集切片
     */
    protected MetadataBaseCollectSplit metadataBaseCollectSplit;

    /**
     * 当前 database
     */
    protected String currentDatabase;

    /**
     * 当前库对应的表集合
     */
    protected List<Object> tableList;

    /**
     * 表集合的迭代器
     */
    protected Iterator<Object> iterator;

    /**
     * 当前表对象
     */
    protected Object currentObject;

    /**
     * 存放所有采集后对象
     */
    protected Queue<MetadataEntity> metadataEntities;

    @Override
    public void init(ISourceDTO sourceDTO, MetadataCollectCondition metadataCollectCondition,
                     MetadataBaseCollectSplit metadataBaseCollectSplit, Queue<MetadataEntity> metadataEntities) throws Exception {
        log.info("init collect Split start: {} ", metadataBaseCollectSplit);
        this.sourceDTO = sourceDTO;
        this.metadataCollectCondition = metadataCollectCondition;
        this.metadataBaseCollectSplit = metadataBaseCollectSplit;
        this.tableList = metadataBaseCollectSplit.getTableList();
        this.currentDatabase = metadataBaseCollectSplit.getDbName();
        this.metadataEntities = metadataEntities;
        // 建立连接
        openConnection();
        this.iterator = tableList.iterator();
        IS_INIT.set(true);
        log.info("init collect Split end : {}", metadataBaseCollectSplit);
    }

    @Override
    public MetadataEntity readNext() {
        currentObject = iterator.next();
        if (!filterTable(currentObject, metadataCollectCondition.getExcludeDatabase(), metadataCollectCondition.getExcludeTable())) {
            MetadataEntity metadataEntity = new MetadataEntity();
            try {
                metadataEntity = createMetadataEntity();
                log.info("\n 采集到的数据: {}", JSON.toJSONString(metadataEntity));
                metadataEntity.setQuerySuccess(true);
            } catch (Exception e) {
                metadataEntity.setSchema(currentDatabase);
                metadataEntity.setTableName(String.valueOf(currentObject));
                metadataEntity.setQuerySuccess(false);
                metadataEntity.setErrorMsg(ExceptionUtils.getStackTrace(e));
            }
            metadataEntity.setOperaType(BaseCons.DEFAULT_OPERA_TYPE);
            return metadataEntity;
        }
        return null;
    }

    @Override
    public boolean reachEnd() {
        return !iterator.hasNext();
    }

    @Override
    public void run() {
        try {
            // 确保执行过 init 方法
            AssertUtils.isTrue(IS_INIT.get(), "metadata collect is not init.");
            Thread.currentThread().setContextClassLoader(this.getClass().getClassLoader());
            while (!reachEnd()) {
                MetadataEntity metadataEntity = readNext();
                if (Objects.nonNull(metadataEntity)) {
                    metadataEntities.offer(metadataEntity);
                }
            }
        } catch (Exception e) {
            log.error("metadata collect error.", e);
        } finally {
            this.close();
            CountDownLatch countDownLatch = metadataBaseCollectSplit.getCountDownLatch();
            // 释放 countDownLatch
            if (Objects.nonNull(countDownLatch)) {
                countDownLatch.countDown();
            }
        }
        log.info("metadata collect end... splits: {}", metadataBaseCollectSplit);
    }

    /**
     * 建立连接
     */
    protected abstract void openConnection();

    /**
     * create meta data entity
     *
     * @return MetadataEntity
     */
    protected abstract MetadataEntity createMetadataEntity();

    /**
     * 获取当前库下面的所有表
     *
     * @return 所有表信息
     */
    protected abstract List<Object> showTables();

    /**
     * 过滤表
     *
     * @param table           当前表实体
     * @param excludeDatabase 过滤的 db
     * @param excludeTable    过滤的表正则
     * @return 是否过滤
     */
    protected boolean filterTable(Object table, List<String> excludeDatabase, List<String> excludeTable) {

        List<Pattern> excludeTablePattern = CollectionUtils.isEmpty(excludeTable) ? Collections.emptyList() : excludeTable.stream().map(Pattern::compile).collect(Collectors.toList());
        // database为空 则匹配table
        if (CollectionUtils.isEmpty(excludeDatabase) && excludeTablePattern.stream().anyMatch(i -> i.matcher((String) table).matches())) {
            log.debug("filter table,dbName is {}, tableName is {} ", currentDatabase, currentObject);
            return true;
        }

        if (CollectionUtils.isNotEmpty(excludeDatabase) && excludeDatabase.contains(currentDatabase)) {
            if (CollectionUtils.isNotEmpty(excludeTablePattern)) {
                if (excludeTablePattern.stream().anyMatch(i -> i.matcher((String) table).matches())) {
                    log.debug("filter table,dbName is {}, tableName is {} ", currentDatabase, currentObject);
                    return true;
                }
            } else {
                log.debug("filter table,dbName is {}, tableName is {} ", currentDatabase, currentObject);
                return true;
            }
        }
        return false;
    }
}
