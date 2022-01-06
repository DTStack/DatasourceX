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

package com.dtstack.dtcenter.common.loader.hive2;

import com.dtstack.dtcenter.common.loader.common.DtClassConsistent;
import com.dtstack.dtcenter.common.loader.common.utils.DBUtil;
import com.dtstack.dtcenter.common.loader.hadoop.util.KerberosLoginUtil;
import com.dtstack.dtcenter.common.loader.rdbms.ConnFactory;
import com.dtstack.dtcenter.loader.dto.source.HiveSourceDTO;
import com.dtstack.dtcenter.loader.dto.source.HiveSslConfig;
import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.dtstack.dtcenter.loader.source.DataBaseType;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;

import java.security.PrivilegedAction;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;
import java.util.Set;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 17:07 2020/1/7
 * @Description：Hive 连接池工厂
 */
@Slf4j
public class HiveConnFactory extends ConnFactory {
    // Hive 属性前缀
    private static final String HIVE_CONF_PREFIX = "hiveconf:";

    private static final String SSL_FLAG = "ssl";

    private static final String SSL_TRUST_STORE = "sslTrustStore";

    private static final String SSL_STORE_PASSWORD = "trustStorePassword";

    public HiveConnFactory() {
        this.driverName = DataBaseType.HIVE.getDriverClassName();
        this.errorPattern = new HiveErrorPattern();
    }

    @Override
    public Connection getConn(ISourceDTO iSource, String taskParams) throws Exception {
        init();
        HiveSourceDTO hiveSourceDTO = (HiveSourceDTO) iSource;

        Connection connection = KerberosLoginUtil.loginWithUGI(hiveSourceDTO.getKerberosConfig()).doAs(
                (PrivilegedAction<Connection>) () -> {
                    try {
                        DriverManager.setLoginTimeout(30);
                        Properties properties = DBUtil.stringToProperties(taskParams);
                        // 特殊处理 properties 属性
                        dealProperties(properties);
                        dealSsl(properties, hiveSourceDTO.getHiveSslConfig());
                        properties.put(DtClassConsistent.PublicConsistent.USER, hiveSourceDTO.getUsername() == null ? "" : hiveSourceDTO.getUsername());
                        properties.put(DtClassConsistent.PublicConsistent.PASSWORD, hiveSourceDTO.getPassword() == null ? "" : hiveSourceDTO.getPassword());
                        String urlWithoutSchema = HiveDriverUtil.removeSchema(hiveSourceDTO.getUrl());
                        return DriverManager.getConnection(urlWithoutSchema, properties);
                    } catch (Exception e) {
                        // 对异常进行统一处理
                        throw new DtLoaderException(errorAdapter.connAdapter(e.getMessage(), errorPattern), e);
                    }
                }
        );

        return HiveDriverUtil.setSchema(connection, hiveSourceDTO.getUrl(), hiveSourceDTO.getSchema());
    }

    /**
     * 处理 Hive 的 Properties 属性
     *
     * @param properties
     */
    private void dealProperties (Properties properties) {
        if (properties == null || properties.isEmpty()) {
            return;
        }

        // 特殊处理 hive 的 key，增加 hiveconf: 属性 key 前缀
        Set<String> keys = properties.stringPropertyNames();
        for (String key : keys) {
            properties.put(HIVE_CONF_PREFIX + key, properties.getProperty(key));
            properties.remove(key);
        }
    }

    /**
     * 处理hive ssl认证信息
     *
     * @param properties
     * @param sslConfig
     */
    private void dealSsl(Properties properties, HiveSslConfig sslConfig) {
        if (sslConfig == null || !sslConfig.getUseSsl()) {
            return;
        }

        if (StringUtils.isBlank(sslConfig.getKeystorePath())) {
            throw new DtLoaderException("hive ssl认证缺少证书路径");
        }

        properties.put(SSL_FLAG, BooleanUtils.toStringTrueFalse(true));
        properties.put(SSL_TRUST_STORE, sslConfig.getKeystorePath());
        if (StringUtils.isNotBlank(sslConfig.getKeystorePassword())) {
            properties.put(SSL_STORE_PASSWORD, sslConfig.getKeystorePassword());
        }
    }
}
