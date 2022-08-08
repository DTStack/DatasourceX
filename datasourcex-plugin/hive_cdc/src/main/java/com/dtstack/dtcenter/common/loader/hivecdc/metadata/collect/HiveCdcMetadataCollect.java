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

package com.dtstack.dtcenter.common.loader.hivecdc.metadata.collect;

import com.dtstack.dtcenter.common.loader.hivecdc.metadata.client.HiveCdcClient;
import com.dtstack.dtcenter.common.loader.hivecdc.metadata.entity.QueueData;
import com.dtstack.dtcenter.common.loader.metadata.collect.AbstractMetaDataCollect;
import com.dtstack.dtcenter.loader.dto.metadata.entity.MetadataEntity;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * hiveCdc元数据采集器
 * 采集逻辑区别于普通的rdbms数据库，只起一个监听线程进行库表的事件变动监听
 *
 * @author luming
 * @date 2022/4/15
 */
@Slf4j
public class HiveCdcMetadataCollect extends AbstractMetaDataCollect {

    private HiveCdcClient client;

    private final BlockingQueue<QueueData> queue = new SynchronousQueue<>(false);

    private int failedTime = 0;

    private ExecutorService executor;

    @Override
    protected void openConnection() {
        tableList = showTables();

        client = new HiveCdcClient(queue);

        ThreadFactory namedThreadFactory = new ThreadFactoryBuilder()
                .setNameFormat("com.dtstack.datasourceX.metadatahivecdc-pool-%d")
                .setUncaughtExceptionHandler((t, e) -> {
                    log.warn("hiveCdcClient run failed.", e);
                    executor.execute(client);
                    log.info("Re-execute hiveCdcClient successfully");
                })
                .build();

        this.executor = new ThreadPoolExecutor(1,
                2,
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(1024),
                namedThreadFactory,
                new ThreadPoolExecutor.AbortPolicy());

        client.init(sourceDTO, metadataCollectCondition.getExcludeDatabase(),
                metadataCollectCondition.getExcludeTable(), metadataCollectCondition.getOriginalJob());
        executor.execute(client);
    }

    @Override
    protected MetadataEntity createMetadataEntity() {
        return null;
    }

    @Override
    protected List<Object> showTables() {
        return Lists.newArrayList();
    }

    @Override
    public void close() {

    }

    @Override
    public boolean reachEnd() {
        try {
            //休眠频率和采集频率保持一致
            TimeUnit.SECONDS.sleep(5);
        } catch (InterruptedException e) {
            //ignore
        }
        return false;
    }

    @Override
    public MetadataEntity readNext() {
        try {
            QueueData data = queue.take();
            if (null != data.getE()) {
                log.error("", data.getE());
                //三次重试失败则抛出异常
                if (++failedTime >= 3) {
                    throw data.getE();
                }
            } else {
                failedTime = 0;
                return data.getMetadataHiveCdcEntity();
            }
        } catch (Exception e) {
            throw new DtLoaderException("get data has interrupted", e);
        }
        return null;
    }
}
