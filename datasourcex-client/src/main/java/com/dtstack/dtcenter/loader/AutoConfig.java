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

package com.dtstack.dtcenter.loader;

import com.dtstack.dtcenter.loader.client.ClientCache;
import com.dtstack.dtcenter.loader.utils.AssertUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

import java.util.Objects;

/**
 * 自动配置类
 *
 * @author ：wangchuan
 * date：Created in 下午2:38 2022/2/22
 * company: www.dtstack.com
 */
@Configuration
@Slf4j
public class AutoConfig implements ApplicationContextAware {

    private static Environment ENVIRONMENT;

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        // 设置 context
        ClientCache.setApplicationContext(applicationContext);

        // 设置 Environment
        Environment environment = applicationContext.getEnvironment();
        if (Objects.isNull(AutoConfig.ENVIRONMENT)) {
            AutoConfig.ENVIRONMENT = environment;
        }

       /* // 设置上传下载 manager
        TransManagerFactory.setExecutorManager(TransManagerImpl.getInstance());*/
    }

    /**
     * 获取 spring 配置
     *
     * @param envKey key
     * @return 配置
     */
    public static String getEnvWithThrow(String envKey) {
        AssertUtils.notNull(AutoConfig.ENVIRONMENT, "environment is null");
        AssertUtils.notBlank(envKey, "envKey can't be null");
        String value = AutoConfig.ENVIRONMENT.getProperty(envKey);
        AssertUtils.notBlank(envKey, String.format("the value of envKey: %s is null", envKey));
        return value;
    }
}
