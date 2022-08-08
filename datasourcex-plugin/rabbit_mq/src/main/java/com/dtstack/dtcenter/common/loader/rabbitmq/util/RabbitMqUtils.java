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

package com.dtstack.dtcenter.common.loader.rabbitmq.util;

import com.dtstack.dtcenter.loader.dto.source.RabbitMqSourceDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.dtstack.dtcenter.loader.utils.AssertUtils;
import com.rabbitmq.client.Address;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.http.client.Client;
import com.rabbitmq.http.client.GetAckMode;
import com.rabbitmq.http.client.GetEncoding;
import com.rabbitmq.http.client.domain.InboundMessage;
import com.rabbitmq.http.client.domain.QueueInfo;
import com.rabbitmq.http.client.domain.VhostInfo;
import lombok.extern.slf4j.Slf4j;
import org.apache.tomcat.util.codec.binary.Base64;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * @author leon
 * @date 2022-07-14 11:44
 **/
@Slf4j
public class RabbitMqUtils {

    private static final Long DEFAULT_CONNECTION_TIMEOUT = TimeUnit.SECONDS.toMillis(1);

    private static final String API_TEMPLATE = "http://%s:%s/api/";

    private static final String SPLIT_ADDRESS_REGEX = " *; *";

    private static final int MAX_POOL_RECORDS = 5;

    public static boolean testCon(RabbitMqSourceDTO sourceDTO) {
        Connection connection = null;
        try {
            connection = getConn(sourceDTO);
            return true;
        } catch (Throwable e) {
            log.error("get rabbitmq connection fail:{}",e.getMessage(),e);
            throw new DtLoaderException(String.format("connect rabbitmq fail: %s", e.getMessage()), e);
        } finally {
            try {
                if (Objects.nonNull(connection)) {
                    connection.close();
                }
            } catch (Throwable e) {
                log.error("rabbitmq connection close fail {}", e.getMessage(), e);
            }
        }
    }

    public static List<String> getVirtualHosts(RabbitMqSourceDTO sourceDTO) {
        assertParam(sourceDTO);
        List<String> result = new ArrayList<>();
        try {
            Client client = getClient(sourceDTO);
            List<VhostInfo> vhosts = client.getVhosts();
            vhosts.forEach(v -> result.add(v.getName()));
        } catch (Throwable e) {
            log.error("get rabbitmq queues fail:{}",e.getMessage(),e);
            throw new DtLoaderException(String.format("get rabbitmq virtualHosts fail: %s", e.getMessage()), e);
        }
        return result;
    }

    public static List<String> getQueues(RabbitMqSourceDTO sourceDTO) {
        assertParam(sourceDTO);
        List<String> result = new ArrayList<>();
        try {
            Client client = getClient(sourceDTO);
            List<QueueInfo> queues = client.getQueues(Optional.ofNullable(sourceDTO.getVirtualHost()).orElse("/"));
            queues.forEach(q -> result.add(q.getName()));
        } catch (Throwable e) {
            log.error("get rabbitmq queues fail:{}",e.getMessage(),e);
            throw new DtLoaderException(String.format("get rabbitmq queues fail: %s", e.getMessage()), e);
        }
        return result;
    }


    public static List<List<Object>> preview(RabbitMqSourceDTO sourceDTO) {
        assertParam(sourceDTO);
        AssertUtils.notBlank(sourceDTO.getQueue(),"queue cannot be null");
        List<List<Object>> result = new ArrayList<>();
        try {
            Client client = getClient(sourceDTO);
            String virtualHost = Optional.ofNullable(sourceDTO.getVirtualHost()).orElse("/");
            String queue = sourceDTO.getQueue();
            Integer previewLimit = sourceDTO.getPreviewLimit();
            if (Objects.isNull(previewLimit) || previewLimit <= 0) {
                previewLimit = MAX_POOL_RECORDS;
            }
            List<InboundMessage> inboundMessages = client.get(virtualHost, queue, previewLimit, GetAckMode.REJECT_REQUEUE_TRUE, GetEncoding.BASE64, -1);
            inboundMessages.forEach(m -> {
                List<Object> message = new ArrayList<>();
                message.add(new String(Base64.decodeBase64(m.getPayload())));
                result.add(message);
            });
        } catch (Throwable e) {
            log.error("get rabbitmq preview fail:{}",e.getMessage(),e);
            throw new DtLoaderException(String.format("get rabbitmq preview fail: %s", e.getMessage()), e);
        }
        return result;
    }


    private static Connection getConn(RabbitMqSourceDTO sourceDTO) throws IOException, TimeoutException {
        assertParam(sourceDTO);
        ConnectionFactory factory = new ConnectionFactory();
        String address = sourceDTO.getAddress();
        String username = sourceDTO.getUsername();
        String password = sourceDTO.getPassword();
        String virtualHost = sourceDTO.getVirtualHost();
        Long connectionTimeout =
                Optional.ofNullable(sourceDTO.getConnectionTimeout())
                        .orElse(DEFAULT_CONNECTION_TIMEOUT);
        List<Address> addresses = parseAddresses(address);
        factory.setAddress(addresses);
        factory.setUsername(username);
        factory.setPassword(password);
        factory.setVirtualHost(virtualHost);
        factory.setConnectionTimeout(Integer.parseInt(connectionTimeout.toString()));
        return factory.newConnection();
    }

    private static Client getClient(RabbitMqSourceDTO sourceDTO) throws MalformedURLException, URISyntaxException {
        String addresses = sourceDTO.getAddress();
        String username = sourceDTO.getUsername();
        String password = sourceDTO.getPassword();
        Integer managementTcpPort = sourceDTO.getManagementTcpPort();
        AssertUtils.notNull(managementTcpPort,"managementTcpPort cannot be null");

        Address address = parseAddresses(addresses).get(0);
        String apiUrl = String.format(API_TEMPLATE, address.getHost(),managementTcpPort);
        return new Client(apiUrl, username, password);
    }


    private static List<Address> parseAddresses(String addresses) {
        String[] addressesArray = split(addresses);
        List<Address> res = new ArrayList<>(addressesArray.length);
        for (String s : addressesArray) {
            res.add(Address.parseAddress(s));
        }
        return res;
    }

    private static String[] split(String addresses) {
        return addresses.split(SPLIT_ADDRESS_REGEX);
    }

    private static void assertParam(RabbitMqSourceDTO rabbitMqSourceDTO) {
        AssertUtils.notBlank(rabbitMqSourceDTO.getAddress(), "address cannot be null");
        AssertUtils.notBlank(rabbitMqSourceDTO.getUsername(), "username cannot be null");
        AssertUtils.notBlank(rabbitMqSourceDTO.getPassword(), "password cannot be null");
    }
}
