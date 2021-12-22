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

import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

import java.sql.Connection;
import java.util.Map;

/**
 * restful 数据源信息
 *
 * @author ：wangchuan
 * date：Created in 下午2:00 2021/8/9
 * company: www.dtstack.com
 */
@Data
@ToString
@SuperBuilder
public class RestfulSourceDTO implements ISourceDTO {

    /**
     * 请求地址
     */
    private String url;

    /**
     * 协议 仅支持 HTTP/HTTPS
     */
    private String protocol;

    /**
     * 请求头信息
     */
    private Map<String, String> headers;

    /**
     * 连接超时时间，单位：秒
     */
    private Integer connectTimeout;

    /**
     * socket 超时时间，单位：秒
     */
    private Integer socketTimeout;

    /**
     * kerberos 配置信息
     */
    private Map<String, Object> kerberosConfig;


    /**
     * sslClient.xml 的绝对路径
     *
     * @return
     */
    private String sslClientConf;

    @Override
    public Connection getConnection() {
        throw new DtLoaderException("The method is not supported");
    }

    @Override
    public void setConnection(Connection connection) {
        throw new DtLoaderException("The method is not supported");
    }

    @Override
    public String getUsername() {
        throw new DtLoaderException("The method is not supported");
    }

    @Override
    public String getPassword() {
        throw new DtLoaderException("The method is not supported");
    }

    @Override
    public Integer getSourceType() {
        return DataSourceType.RESTFUL.getVal();
    }
}
