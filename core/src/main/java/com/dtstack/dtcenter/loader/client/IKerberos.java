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

package com.dtstack.dtcenter.loader.client;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 10:15 2020/8/25
 * @Description：Kerberos 操作类
 */
public interface IKerberos {
    /**
     * 从 ZIP 包中解压出 Kerberos 配置信息
     * 其中地址相关信息因为 SFTP 的原因，只存储相对路径，在校验之前再做转化
     * 调用 #{@link #prepareKerberosForConnect}
     *
     * @param zipLocation
     * @param localKerberosPath
     * @return
     * @throws Exception
     */
    Map<String, Object> parseKerberosFromUpload(String zipLocation, String localKerberosPath) throws IOException;

    /**
     * 连接 Kerberos 前的准备工作
     * 1. 会替换相对路径到绝对路径，目前支持的是存在一个或者一个 / 都不存在的情况
     * 2. 会增加或者修改一些 Principal 参数
     *
     * @param conf
     * @param localKerberosPath
     * @return
     * @throws Exception
     */
    Boolean prepareKerberosForConnect(Map<String, Object> conf, String localKerberosPath);

    /**
     * 从 JDBC URL 中获取 Principal
     *
     * @param url
     * @return
     * @throws Exception
     */
    String getPrincipals(String url);

    /**
     * 从 Kerberos 配置文件中获取 Principal
     *
     * @param kerberosConfig
     * @return
     * @throws Exception
     */
    List<String> getPrincipals(Map<String, Object> kerberosConfig);
}
