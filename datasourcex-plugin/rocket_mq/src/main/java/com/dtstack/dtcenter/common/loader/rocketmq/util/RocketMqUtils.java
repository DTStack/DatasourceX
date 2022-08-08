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

package com.dtstack.dtcenter.common.loader.rocketmq.util;

import com.dtstack.dtcenter.loader.dto.source.RocketMqSourceDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.google.common.collect.Lists;
import org.apache.commons.lang.StringUtils;
import org.apache.rocketmq.acl.common.AclClientRPCHook;
import org.apache.rocketmq.acl.common.SessionCredentials;
import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.client.consumer.DefaultLitePullConsumer;
import org.apache.rocketmq.client.impl.MQAdminImpl;
import org.apache.rocketmq.client.impl.MQClientAPIImpl;
import org.apache.rocketmq.client.impl.MQClientManager;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.body.TopicList;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.remoting.RPCHook;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author leon
 * @date 2022-06-20 15:15
 **/
public class RocketMqUtils {

    private static final String CONSUMER_GROUP = "STREAM_APP_ROCKET_MQ";

    public static final String EARLIEST = "earliest";

    private static final int MAX_POOL_RECORDS = 5;

    public static boolean checkConnection(RocketMqSourceDTO sourceDTO) {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setNamesrvAddr(sourceDTO.getNameServerAddress());
        MQClientInstance mqClientInstance = MQClientManager.getInstance().getOrCreateMQClientInstance(clientConfig, getAclRPCHook(sourceDTO));
        try {
            mqClientInstance.start();
            mqClientInstance.getMQClientAPIImpl().getSystemTopicList(1000);
            return true;
        } catch (Exception e) {
            throw new DtLoaderException(String.format("connect rocketmq fail: %s", e.getMessage()), e);
        } finally {
            mqClientInstance.shutdown();
        }
    }


    public static List<String> getTopicList(RocketMqSourceDTO rocketMqSourceDTO, Boolean containSystemTopic) {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setNamesrvAddr(rocketMqSourceDTO.getNameServerAddress());
        MQClientInstance mqClientInstance = MQClientManager.getInstance().getOrCreateMQClientInstance(clientConfig, getAclRPCHook(rocketMqSourceDTO));
        try {
            mqClientInstance.start();
            MQClientAPIImpl mqClientAPI = mqClientInstance.getMQClientAPIImpl();
            TopicList topicList = mqClientAPI.getTopicListFromNameServer(1000);
            if (containSystemTopic) {
                return Lists.newArrayList(topicList.getTopicList());
            }
            // filter system topic
            TopicList systemTopicList = mqClientAPI.getSystemTopicList(1000);
            Set<String> system = systemTopicList.getTopicList();
            return topicList.getTopicList().stream().filter(e -> !system.contains(e)).collect(Collectors.toList());
        } catch (Exception e) {
            throw new DtLoaderException(e.getMessage(), e);
        } finally {
            mqClientInstance.shutdown();
        }
    }


    public static List<List<Object>> preview(RocketMqSourceDTO rocketMqSourceDTO, String prevMode) {
        List<List<Object>> result = new ArrayList<>();
        List<Object> messageBody = new ArrayList<>();
        List<MessageExt> messages = poll(rocketMqSourceDTO, prevMode);
        for (MessageExt messageExt : messages) {
            byte[] body = messageExt.getBody();
            messageBody.add(new String(body, StandardCharsets.UTF_8));
        }
        result.add(messageBody);
        return result;
    }


    private static List<MessageExt> poll(RocketMqSourceDTO rocketMqSourceDTO, String prevMode) {
        DefaultLitePullConsumer consumer = new DefaultLitePullConsumer(CONSUMER_GROUP,getAclRPCHook(rocketMqSourceDTO));
        List<MessageExt> result = new ArrayList<>();
        try {
            consumer.setNamesrvAddr(rocketMqSourceDTO.getNameServerAddress());
            consumer.start();
            // fetch queue
            Collection<MessageQueue> queues = consumer.fetchMessageQueues(rocketMqSourceDTO.getTopic());
            // assign
            consumer.assign(queues,rocketMqSourceDTO.getTopic(),getTag(rocketMqSourceDTO));

            ClientConfig clientConfig = consumer.cloneClientConfig();
            MQClientInstance mqClientInstance = MQClientManager.getInstance().getOrCreateMQClientInstance(clientConfig, getAclRPCHook(rocketMqSourceDTO));
            MQAdminImpl mqAdmin = mqClientInstance.getMQAdminImpl();

            for (MessageQueue queue:queues) {
                if (EARLIEST.equals(prevMode)) {
                    consumer.seekToBegin(queue);
                } else {
                    // max Offset of queue
                    long maxOffset = mqAdmin.maxOffset(queue);
                    long offset = maxOffset - MAX_POOL_RECORDS;
                    offset = offset > 0 ? offset : 0;
                    consumer.seek(queue, offset);
                }
            }

            // poll
            List<MessageExt> poll = new ArrayList<>();
            while (true) {
                if (poll.size() >= MAX_POOL_RECORDS) {
                    break;
                }
                List<MessageExt> messages = consumer.poll(1000);
                if (messages.size() == 0) {
                    break;
                }
                poll.addAll(messages);
            }

            for (MessageExt message:poll) {
                if (result.size() >= MAX_POOL_RECORDS) {
                    break;
                }
                result.add(message);
            }

        } catch (Exception e) {
            throw new DtLoaderException(e.getMessage(), e);
        } finally {
            consumer.shutdown();
        }

        return result;
    }

    private static String getTag(RocketMqSourceDTO rocketMqSourceDTO) {
        String tag = rocketMqSourceDTO.getTag();
        if (StringUtils.isBlank(tag) || SubscriptionData.SUB_ALL.equals(tag)) {
            return SubscriptionData.SUB_ALL;
        }
        return tag;
    }


    static RPCHook getAclRPCHook(RocketMqSourceDTO sourceDTO) {
        if (StringUtils.isBlank(sourceDTO.getAccessKey()) || StringUtils.isBlank(sourceDTO.getSecretKey())) {
            return null;
        }
        return new AclClientRPCHook(new SessionCredentials(sourceDTO.getAccessKey(), sourceDTO.getSecretKey()));
    }

}
