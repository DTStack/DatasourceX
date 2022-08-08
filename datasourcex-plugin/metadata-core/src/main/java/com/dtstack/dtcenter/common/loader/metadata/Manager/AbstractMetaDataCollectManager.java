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

package com.dtstack.dtcenter.common.loader.metadata.Manager;

import com.alibaba.fastjson.JSON;
import com.dtstack.dtcenter.common.loader.common.DtClassThreadFactory;
import com.dtstack.dtcenter.common.loader.metadata.collect.MetaDataCollect;
import com.dtstack.dtcenter.common.loader.metadata.constants.BaseCons;
import com.dtstack.dtcenter.common.loader.metadata.core.MetadataBaseCollectSplit;
import com.dtstack.dtcenter.loader.dto.metadata.MetadataCollectCondition;
import com.dtstack.dtcenter.loader.dto.metadata.entity.MetadataEntity;
import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;
import com.dtstack.dtcenter.loader.metadata.MetaDataCollectManager;
import com.dtstack.dtcenter.loader.utils.AssertUtils;
import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * metadata base collect
 *
 * @author ：wangchuan
 * date：Created in 上午9:22 2022/4/6
 * company: www.dtstack.com
 */
@Slf4j
public abstract class AbstractMetaDataCollectManager implements MetaDataCollectManager {

    private static final Integer PARALLELISM = 5;

    private static final Integer MAX_PARALLELISM = 20;

    private static final AtomicInteger POOL_ID = new AtomicInteger();

    /**
     * 元数据采集条件
     */
    public MetadataCollectCondition metadataCollectCondition;

    /**
     * 元数据采集任务线程池
     */
    private ExecutorService executorService = null;

    /**
     * 所有的任务队列
     */
    private final List<MetaDataCollect> metaDataCollects = Lists.newArrayList();

    /**
     * 任务锁
     */
    private CountDownLatch countDownLatch = null;

    /**
     * 存放所有采集到的数据对象
     */
    private final Queue<MetadataEntity> metadataEntities = new ConcurrentLinkedQueue<>();

    private Consumer<List<MetadataEntity>> collectConsumer;

    protected final ISourceDTO sourceDTO;

    public AbstractMetaDataCollectManager(ISourceDTO sourceDTO) {
        this.sourceDTO = sourceDTO;
    }

    @Override
    public void init(MetadataCollectCondition metadataCollectCondition, Consumer<List<MetadataEntity>> collectConsumer) throws Exception {
        AssertUtils.notNull(metadataCollectCondition, "metadata collect condition can't be null.");
        AssertUtils.notNull(collectConsumer, "collect consumer can't be null.");
        this.metadataCollectCondition = metadataCollectCondition;
        this.collectConsumer = collectConsumer;
        // 划分切片
        List<MetadataBaseCollectSplit> collectSplits = createCollectSplits();

        if (CollectionUtils.isEmpty(collectSplits)) {
            log.warn("collect splits is empty, metadataCollectCondition : {}", JSON.toJSONString(metadataCollectCondition));
            return;
        }

        // 获取元数据采集并行度, 并行度的大小决定了线程池的大小
        Integer parallelism = getCollectParallelism(collectSplits.size(), metadataCollectCondition.getParallelism(), metadataCollectCondition.getMaxParallelism());

        // 构建线程池处理元数据采集
        executorService = new ThreadPoolExecutor(parallelism, parallelism, 0,
                TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(1000),
                new DtClassThreadFactory("ExecMetadataCollectTask-" + POOL_ID.getAndIncrement() + "-" + this.getClass().getName()));

        countDownLatch = new CountDownLatch(collectSplits.size());
        log.info("parallelism : {}, collectSplitsSize : {}", parallelism, collectSplits.size());

        for (MetadataBaseCollectSplit collectSplit : collectSplits) {
            collectSplit.setCountDownLatch(countDownLatch);
            // 反射创建 MetaDataCollect
            MetaDataCollect metaDataCollect = getMetadataCollectClass().newInstance();
            metaDataCollect.init(sourceDTO, metadataCollectCondition, collectSplit, metadataEntities);
            metaDataCollects.add(metaDataCollect);
        }
    }

    protected abstract Class<? extends MetaDataCollect> getMetadataCollectClass();

    @Override
    public boolean isEnd() {
        try {
            // sleep 1s, 防止循环调用
            TimeUnit.SECONDS.sleep(1);
        } catch (InterruptedException e) {
            log.info("sleep error.", e);
        }

        //防止数据积压过多，根据batchInterval参数执行一次回调
        if (CollectionUtils.isNotEmpty(metadataEntities)
                && metadataEntities.size() >= metadataCollectCondition.getBatchInterval()) {
            acceptBatchConsumer();
        }

        // countDownLatch 为 0 表示为执行完毕
        if (countDownLatch.getCount() == 0) {
            while (CollectionUtils.isNotEmpty(metadataEntities)) {
                acceptBatchConsumer();
            }
            return true;
        }
        return false;
    }

    @Override
    public void start() {
        AssertUtils.notNull(executorService, "executor service cant't be null.");
        AssertUtils.notNull(countDownLatch, "countDownLatch cant't be null.");
        if (CollectionUtils.isNotEmpty(metaDataCollects)) {
            // 提交到线程池处理
            metaDataCollects.forEach(executorService::submit);
        }
    }

    @Override
    public void stop() {
        log.info("stop collect...");
        // 关闭线程池
        executorService.shutdownNow();
        // 关闭 metadata collect
        metaDataCollects.forEach(MetaDataCollect::close);
        // 初始化 countDownLatch
        countDownLatch = new CountDownLatch(0);
    }

    /**
     * 获取元数据采集并行度
     *
     * @param splitNum       切片数量
     * @param parallelism    并行度
     * @param maxParallelism 最大并行度
     * @return 元数据采集并行度
     */
    protected Integer getCollectParallelism(int splitNum, Integer parallelism, Integer maxParallelism) {
        Integer p = Objects.isNull(parallelism) ? PARALLELISM : parallelism;
        Integer mp = Objects.isNull(maxParallelism) ? MAX_PARALLELISM : maxParallelism;
        AssertUtils.isTrue(p <= mp, "parallelism can't greater than maxParallelism.");
        if (splitNum == 0) {
            return parallelism;
        } else if (splitNum <= p) {
            return splitNum;
        } else if (splitNum >= mp) {
            return mp;
        } else {
            return p;
        }
    }

    /**
     * 划分切片
     *
     * @return 划分后的切片
     */
    @SuppressWarnings("unchecked")
    protected List<MetadataBaseCollectSplit> createCollectSplits() {
        List<Map<String, Object>> originalJob = metadataCollectCondition.getOriginalJob();
        AssertUtils.notNull(originalJob, "originalJob can't be null.");
        // 过滤 database
        filterDatabase();

        List<MetadataBaseCollectSplit> collectSplits = Lists.newArrayList();
        originalJob.forEach(dbTables -> {
            String dbName = MapUtils.getString(dbTables, BaseCons.KEY_DB_NAME);
            if (StringUtils.isNotEmpty(dbName)) {
                List<Object> tables = (List<Object>) dbTables.get(BaseCons.KEY_TABLE_LIST);
                MetadataBaseCollectSplit collectSplit = MetadataBaseCollectSplit.builder()
                        .dbName(dbName)
                        .tableList(tables).build();
                collectSplits.add(collectSplit);
            }
        });

        return collectSplits;
    }

    /**
     * 过滤 database
     */
    private void filterDatabase() {
        //过滤表为空 过滤database不为空，则把对应的database去除 不需要同步
        if (CollectionUtils.isEmpty(metadataCollectCondition.getExcludeTable()) && CollectionUtils.isNotEmpty(metadataCollectCondition.getExcludeDatabase())) {
            List<Map<String, Object>> job = new ArrayList<>(metadataCollectCondition.getOriginalJob().size());
            for (Map<String, Object> map : metadataCollectCondition.getOriginalJob()) {
                String dbName = MapUtils.getString(map, BaseCons.KEY_DB_NAME);
                if (StringUtils.isNotEmpty(dbName) && !metadataCollectCondition.getExcludeDatabase().contains(dbName)) {
                    job.add(map);
                } else if (StringUtils.isEmpty(dbName)) {
                    job.add(map);
                }
            }
            metadataCollectCondition.setOriginalJob(job);
        }
    }

    /**
     * 按照设置的batch数值进行回调
     */
    private void acceptBatchConsumer() {
        Integer batch = metadataCollectCondition.getBatchInterval();
        List<MetadataEntity> sendList = Lists.newArrayList();
        for (int i = 0; i < batch; i++) {
            sendList.add(metadataEntities.poll());
        }
        collectConsumer.accept(sendList);
    }
}
