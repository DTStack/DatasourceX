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

package com.dtstack.source.loader.server.config;


import com.dtstack.dtcenter.loader.config.TransManagerImpl;
import com.dtstack.dtcenter.loader.utils.SystemPropertyUtil;
import com.dtstack.dtcenter.loader.cache.client.RealClientCache;
import com.dtstack.rpc.manager.TransManagerFactory;
import lombok.Data;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.File;

/**
 * 环境配置相关
 *
 * @author ：wangchuan
 * date：Created in 下午3:38 2021/12/17
 * company: www.dtstack.com
 */
@Data
@Component
public class EnvironmentContext implements InitializingBean {

    /**
     * server 启动端口
     */
    @Value("${remote.listen.port:8999}")
    private Integer remoteListenPort;

    /**
     * 数据源插件地址
     */
    @Value("${datasourcex.plugin.path:}")
    private String dataSourceXPluginPath;

    /**
     * HADOOP_USER_NAME
     */
    @Value("${hadoop.user.name:admin}")
    private String hadoopUserName;

    /**
     * kerberos 下载路径
     */
    @Value("${kerberos.local.path:}")
    private String kerberosLocalPath;

    /**
     * 文件上传路径
     */
    @Value("${remote.interim.file.path:/remote/tmp/}")
    private String remoteInterimFilePath;

    /**
     * 默认插件包名称
     */
    private static final String PLUGIN_LIB_NAME = "pluginLibs";

    /**
     * 默认 kerberos 下载目录名称
     */
    private static final String KERBEROS_PATH_NAME = "kerberosConf";

    /**
     * 默认 kerberos 下载路径系统变量名称
     */
    private static final String KERBEROS_CONF_DIR = "KERBEROS_CONF_DIR";

    /**
     * 文件上传路径系统变量名称
     */
    private static final String UPLOAD_PATH = "FILE_UPLOAD_PATH";

    @Override
    public void afterPropertiesSet() {
        SystemPropertyUtil.setHadoopUserName(hadoopUserName);
        String pluginPath;
        if (StringUtils.isBlank(dataSourceXPluginPath)) {
            pluginPath = System.getProperty("user.dir") + File.separator + PLUGIN_LIB_NAME + File.separator;
        } else {
            pluginPath = dataSourceXPluginPath;
        }
        RealClientCache.setUserDir(pluginPath);

        String kerberosPath;
        if (StringUtils.isBlank(kerberosLocalPath)) {
            kerberosPath = System.getProperty("user.dir") + File.separator + KERBEROS_PATH_NAME + File.separator;
        } else {
            kerberosPath = kerberosLocalPath;
        }

        System.setProperty(KERBEROS_CONF_DIR, kerberosPath);

        System.setProperty(UPLOAD_PATH, remoteInterimFilePath);

        // 设置上传下载 manager
        TransManagerFactory.setExecutorManager(TransManagerImpl.getInstance());
    }
}
