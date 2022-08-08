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

package com.dtstack.dtcenter.loader.dto.source;

import com.dtstack.dtcenter.loader.source.DataSourceType;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

/**
 * @author leon
 * @date 2022-06-20 15:06
 **/
@Data
@ToString
@SuperBuilder
@NoArgsConstructor
@AllArgsConstructor
public class RabbitMqSourceDTO extends AbstractSourceDTO {

    /**
     * 用户名，必传
     */
    private String username;

    /**
     * 密码，必传
     */
    private String password;

    /**
     * rabbitmq 地址，必传，；隔开
     */
    private String address;

    /**
     * 默认值: /
     */
    private String virtualHost;

    /**
     * 队列名，调用 {@link com.dtstack.dtcenter.loader.client.IRabbitMq#getPreview(com.dtstack.dtcenter.loader.dto.source.ISourceDTO)} 时必传
     *
     */
    private String queue;

    /**
     * getPreview 消息条数限制，默认值 5
     */
    private Integer previewLimit;

    /**
     * 获取连接的超时时间(ms)，默认值 1000
     */
    private Long connectionTimeout;

    /**
     * 管理界面端口号
     */
    private Integer managementTcpPort;

    @Override
    public Integer getSourceType() {
        return DataSourceType.RABBIT_MQ.getVal();
    }
}
