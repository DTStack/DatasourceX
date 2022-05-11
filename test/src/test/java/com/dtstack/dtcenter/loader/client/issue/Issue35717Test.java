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

package com.dtstack.dtcenter.loader.client.issue;

import com.dtstack.dtcenter.loader.client.BaseTest;
import com.dtstack.dtcenter.loader.client.ClientCache;
import com.dtstack.dtcenter.loader.client.IKafka;
import com.dtstack.dtcenter.loader.client.IKerberos;
import com.dtstack.dtcenter.loader.client.sql.KafkaKerberosTest;
import com.dtstack.dtcenter.loader.dto.source.KafkaSourceDTO;
import com.dtstack.dtcenter.loader.kerberos.HadoopConfTool;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * bug 链接 http://zenpms.dtstack.cn/zentao/bug-view-35717.html
 *
 * @author ：wangchuan
 * date：Created in 上午11:01 2021/3/22
 * company: www.dtstack.com
 */
public class Issue35717Test extends BaseTest {

    private static final KafkaSourceDTO SOURCE_DTO = KafkaSourceDTO.builder()
            .brokerUrls("eng-cdh2:9092")
            .build();

    @Before
    public void setUp() throws Exception {
        // 准备 Kerberos 参数
        Map<String, Object> kerberosConfig = new HashMap<>();
        kerberosConfig.put(HadoopConfTool.PRINCIPAL_FILE, "/kafka_eng2.keytab");
        kerberosConfig.put(HadoopConfTool.PRINCIPAL, "kafka/eng-cdh2@DTSTACK.COM");
        kerberosConfig.put(HadoopConfTool.KAFKA_KERBEROS_SERVICE_NAME, "kafka");
        kerberosConfig.put(HadoopConfTool.KEY_JAVA_SECURITY_KRB5_CONF, "/krb5.conf");
        SOURCE_DTO.setKerberosConfig(kerberosConfig);
        String localKerberosPath = KafkaKerberosTest.class.getResource("/eng-cdh2").getPath();
        IKerberos kerberos = ClientCache.getKerberos(DataSourceType.KAFKA.getVal());
        kerberos.prepareKerberosForConnect(kerberosConfig, localKerberosPath);
    }

    @Test
    public void test_for_issue () throws InterruptedException {
        IKafka client = ClientCache.getKafka(DataSourceType.KAFKA_09.getVal());
        ExecutorService threadPool = Executors.newFixedThreadPool(8);
        CountDownLatch countDownLatch = new CountDownLatch(8);
        AtomicInteger checkNum = new AtomicInteger(0);
        for (int i = 0; i < 8; i++) {
            threadPool.submit(() -> {
                Boolean check = client.testCon(SOURCE_DTO);
                if (check) {
                    checkNum.addAndGet(1); ;
                }
                countDownLatch.countDown();
            });
        }
        countDownLatch.await(1, TimeUnit.MINUTES);
        Assert.assertEquals(8, checkNum.get());
        threadPool.shutdownNow();
    }
}
