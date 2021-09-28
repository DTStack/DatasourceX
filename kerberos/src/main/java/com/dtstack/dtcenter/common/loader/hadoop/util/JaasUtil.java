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

package com.dtstack.dtcenter.common.loader.hadoop.util;

import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.dtstack.dtcenter.loader.kerberos.HadoopConfTool;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.Map;

/**
 * Jaas 文件工具类
 *
 * @author ：wangchuan
 * date：Created in 下午5:37 2021/6/2
 * company: www.dtstack.com
 */
@Slf4j
public class JaasUtil {

    /**
     * JAAS CONF 内容
     */
    public static final String JAAS_CONTENT = "Client {\n" +
            "    com.sun.security.auth.module.Krb5LoginModule required\n" +
            "    useKeyTab=true\n" +
            "    storeKey=true\n" +
            "    keyTab=\"%s\"\n" +
            "    useTicketCache=false\n" +
            "    principal=\"%s\";\n" +
            "};";

    /**
     * 写 jaas文件，同时处理 krb5.conf
     *
     * @param kerberosConfig kerberos 配置文件
     * @return jaas文件绝对路径
     */
    public synchronized static String writeJaasConf(Map<String, Object> kerberosConfig) {
        log.info("初始化 jaas.conf 文件, kerberosConfig : {}", kerberosConfig);
        if (MapUtils.isEmpty(kerberosConfig)) {
            return null;
        }

        // 处理 krb5.conf
        if (kerberosConfig.containsKey(HadoopConfTool.KEY_JAVA_SECURITY_KRB5_CONF)) {
            System.setProperty(HadoopConfTool.KEY_JAVA_SECURITY_KRB5_CONF, MapUtils.getString(kerberosConfig, HadoopConfTool.KEY_JAVA_SECURITY_KRB5_CONF));
        }
        String keytabPath = MapUtils.getString(kerberosConfig, HadoopConfTool.PRINCIPAL_FILE);
        try {
            File file = new File(keytabPath);
            File jaas = new File(file.getParent() + File.separator + "jaas.conf");
            if (jaas.exists()) {
                boolean deleteCheck = jaas.delete();
                if (!deleteCheck) {
                    log.error("delete file: {} fail", jaas.getAbsolutePath());
                }
            }
            String principal = MapUtils.getString(kerberosConfig, HadoopConfTool.PRINCIPAL);
            FileUtils.write(jaas, String.format(JAAS_CONTENT, keytabPath, principal));
            String loginConf = jaas.getAbsolutePath();
            log.info("Init Kerberos:login-conf:{}\n principal:{}", keytabPath, principal);
            return loginConf;
        } catch (IOException e) {
            throw new DtLoaderException(String.format("写入 jaas.conf 配置文件异常: %s", e.getMessage()), e);
        }
    }
}
