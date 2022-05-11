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

import java.sql.Connection;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 17:31 2020/5/22
 * @Description：FTP 数据源信息
 */
@Data
@ToString
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class FtpSourceDTO implements ISourceDTO {
    /**
     * 用户名
     */
    private String username;

    /**
     * 密码
     */
    private String password;

    /**
     * 数据源类型
     */
    protected Integer sourceType;

    /**
     * 是否缓存
     */
    @Builder.Default
    protected Boolean isCache = false;

    /**
     * 地址
     */
    private String url;

    /**
     * 端口号
     */
    private String hostPort;

    /**
     * 协议
     */
    private String protocol;

    /**
     * 认证
     */
    private String auth;

    /**
     * 目录
     * FTP rsa 路径
     */
    private String path;

    /**
     * 连接模式
     */
    private String connectMode;


    @Override
    public Integer getSourceType() {
        return DataSourceType.FTP.getVal();
    }

    @Override
    public Connection getConnection() {
        throw new DtLoaderException("The method is not supported");
    }

    @Override
    public void setConnection(Connection connection) {
        throw new DtLoaderException("The method is not supported");
    }
}
