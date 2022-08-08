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

import com.dtstack.dtcenter.loader.cache.client.RealClientCache;
import com.dtstack.dtcenter.loader.constant.CommonConstant;
import com.dtstack.dtcenter.loader.enums.DeployMode;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationContext;

/**
 * client cache 缓存
 *
 * @author ：wangchuan
 * date：Created in 上午10:28 2021/12/20
 * company: www.dtstack.com
 */
@Slf4j
public class ClientCache {

    private static ApplicationContext applicationContext;

    // 通过系统变量设置启动方式
    private static final DeployMode DEPLOY_MODE = DeployMode.getDeployMode(System.getProperty(CommonConstant.DEPLOY_MODE_KEY, DeployMode.LOCAL.getType()));

    static {
        log.info("DatasourceX current call mode : {}", DEPLOY_MODE);
    }

    /**
     * 获取当前部署模式
     *
     * @return 当前部署模式
     */
    public static DeployMode getDeployMode() {
        return DEPLOY_MODE;
    }

    /**
     * 设置插件包路径, 本地调用模式时使用
     *
     * @param pluginPath 插件包路径
     */
    public static void setPluginPath(String pluginPath) {
        RealClientCache.setUserDir(pluginPath);
    }

    /**
     * 获取 Sql Client 客户端
     *
     * @param dataSourceType 数据源类型 val
     * @return IClient
     */
    public static IClient getClient(Integer dataSourceType) {
        if (DeployMode.LOCAL.equals(DEPLOY_MODE)) {
            return RealClientCache.getClient(dataSourceType);
        }
        return applicationContext.getBean(IClient.class);
    }

    /**
     * 获取 Sql Client 客户端
     *
     * @param pluginName 插件包目录名称
     * @return IClient
     */
    public static IClient getClient(String pluginName) {
        return getClient(DataSourceType.getSourceTypeByPluginName(pluginName).getVal());
    }

    /**
     * 获取 HDFS 文件客户端
     *
     * @param dataSourceType 数据源类型 val
     * @return hdfs 文件客户端
     */
    public static IHdfsFile getHdfs(Integer dataSourceType) {
        if (DeployMode.LOCAL.equals(DEPLOY_MODE)) {
            return RealClientCache.getHdfs(dataSourceType);
        }
        return applicationContext.getBean(IHdfsFile.class);
    }

    /**
     * 获取 HDFS 文件客户端
     *
     * @param pluginName 插件包目录名称
     * @return hdfs 文件客户端
     */
    public static IHdfsFile getHdfs(String pluginName) {
        return getHdfs(DataSourceType.getSourceTypeByPluginName(pluginName).getVal());
    }

    /**
     * 获取 KAFKA 客户端
     *
     * @param dataSourceType 数据源类型 val
     * @return kafka 客户端
     */
    public static IKafka getKafka(Integer dataSourceType) {
        if (DeployMode.LOCAL.equals(DEPLOY_MODE)) {
            return RealClientCache.getKafka(dataSourceType);
        }
        return applicationContext.getBean(IKafka.class);
    }

    /**
     * 获取 ROCKET MQ 客户端
     *
     * @param dataSourceType 数据源类型 val
     * @return rocketmq 客户端
     */
    public static IRocketMq getRocketMq(Integer dataSourceType) {
        if (DeployMode.LOCAL.equals(DEPLOY_MODE)) {
            return RealClientCache.getRocketMq(dataSourceType);
        }
        return applicationContext.getBean(IRocketMq.class);
    }

    /**
     * 获取 RABBIT MQ 客户端
     *
     * @param dataSourceType 数据源类型 val
     * @return rabbitmq 客户端
     */
    public static IRabbitMq getRabbitMq(Integer dataSourceType) {
        if (DeployMode.LOCAL.equals(DEPLOY_MODE)) {
            return RealClientCache.getRabbitMq(dataSourceType);
        }
        return applicationContext.getBean(IRabbitMq.class);
    }



    /**
     * 获取 KAFKA 客户端
     *
     * @param pluginName 插件包目录名称
     * @return kafka 客户端
     */
    public static IKafka getKafka(String pluginName) {
        return getKafka(DataSourceType.getSourceTypeByPluginName(pluginName).getVal());
    }

    /**
     * 获取 Kerberos 服务客户端
     *
     * @param dataSourceType 数据源类型 val
     * @return Kerberos 服务客户端
     */
    public static IKerberos getKerberos(Integer dataSourceType) {
        if (DeployMode.LOCAL.equals(DEPLOY_MODE)) {
            return RealClientCache.getKerberos(dataSourceType);
        }
        return applicationContext.getBean(IKerberos.class);
    }

    /**
     * 获取 Kerberos 服务客户端
     *
     * @param pluginName 插件包目录名称
     * @return Kerberos 服务客户端
     */
    public static IKerberos getKerberos(String pluginName) {
        return getKerberos(DataSourceType.getSourceTypeByPluginName(pluginName).getVal());
    }

    /**
     * 获取 hbase 服务客户端
     *
     * @param dataSourceType 数据源类型 val
     * @return hbase新客户端
     */
    public static IHbase getHbase(Integer dataSourceType) {
        if (DeployMode.LOCAL.equals(DEPLOY_MODE)) {
            return RealClientCache.getHbase(dataSourceType);
        }
        return applicationContext.getBean(IHbase.class);
    }

    /**
     * 获取 hbase 服务客户端
     *
     * @param pluginName 插件包目录名称
     * @return hbase新客户端
     */
    public static IHbase getHbase(String pluginName) {
        return getHbase(DataSourceType.getSourceTypeByPluginName(pluginName).getVal());
    }

    /**
     * 获取 table Client 客户端
     *
     * @param dataSourceType 数据源类型 val
     * @return table 客户端
     */
    public static ITable getTable(Integer dataSourceType) {
        if (DeployMode.LOCAL.equals(DEPLOY_MODE)) {
            return RealClientCache.getTable(dataSourceType);
        }
        return applicationContext.getBean(ITable.class);
    }

    /**
     * 获取 table Client 客户端
     *
     * @param pluginName 插件包目录名称
     * @return table 客户端
     */
    public static ITable getTable(String pluginName) {
        return getTable(DataSourceType.getSourceTypeByPluginName(pluginName).getVal());
    }

    /**
     * 获取 tsdb Client 客户端
     *
     * @param dataSourceType 数据源类型 val
     * @return tsdb Client 客户端
     */
    public static ITsdb getTsdb(Integer dataSourceType) {
        if (DeployMode.LOCAL.equals(DEPLOY_MODE)) {
            return RealClientCache.getTsdb(dataSourceType);
        }
        return applicationContext.getBean(ITsdb.class);
    }

    /**
     * 获取 tsdb Client 客户端
     *
     * @param pluginName 插件包目录名称
     * @return tsdb Client 客户端
     */
    public static ITsdb getTsdb(String pluginName) {
        return getTsdb(DataSourceType.getSourceTypeByPluginName(pluginName).getVal());
    }

    /**
     * 获取 restful Client 客户端
     *
     * @param dataSourceType 数据源类型 val
     * @return restful Client 客户端
     */
    public static IRestful getRestful(Integer dataSourceType) {
        if (DeployMode.LOCAL.equals(DEPLOY_MODE)) {
            return RealClientCache.getRestful(dataSourceType);
        }
        return applicationContext.getBean(IRestful.class);
    }

    /**
     * 获取 restful Client 客户端
     *
     * @param pluginName 插件包目录名称
     * @return restful Client 客户端
     */
    public static IRestful getRestful(String pluginName) {
        return getRestful(DataSourceType.getSourceTypeByPluginName(pluginName).getVal());
    }

    /**
     * 获取 redis Client 客户端
     *
     * @param dataSourceType 数据源类型 val
     * @return redis Client 客户端
     */
    public static IRedis getRedis(Integer dataSourceType) {
        if (DeployMode.LOCAL.equals(DEPLOY_MODE)) {
            return RealClientCache.getRedis(dataSourceType);
        }
        return applicationContext.getBean(IRedis.class);
    }

    /**
     * 获取 redis Client 客户端
     *
     * @param pluginName 插件包目录名称
     * @return redis Client 客户端
     */
    public static IRedis getRedis(String pluginName) {
        return getRedis(DataSourceType.getSourceTypeByPluginName(pluginName).getVal());
    }

    /**
     * 获取 job Client 客户端
     *
     * @param dataSourceType 数据源类型 val
     * @return job Client 客户端
     */
    public static IJob getJob(Integer dataSourceType) {
        if (DeployMode.LOCAL.equals(DEPLOY_MODE)) {
            return RealClientCache.getJob(dataSourceType);
        }
        return applicationContext.getBean(IJob.class);
    }

    /**
     * 获取 job Client 客户端
     *
     * @param pluginName 插件包目录名称
     * @return job Client 客户端
     */
    public static IJob getJob(String pluginName) {
        return getJob(DataSourceType.getSourceTypeByPluginName(pluginName).getVal());
    }

    /**
     * 获取 yarn Client 客户端
     *
     * @param dataSourceType 数据源类型 val
     * @return yarn Client 客户端
     */
    public static IYarn getYarn(Integer dataSourceType) {
        if (DeployMode.LOCAL.equals(DEPLOY_MODE)) {
            return RealClientCache.getYarn(dataSourceType);
        }
        return applicationContext.getBean(IYarn.class);
    }

    /**
     * 获取 yarn Client 客户端
     *
     * @param pluginName 插件包目录名称
     * @return yarn Client 客户端
     */
    public static IYarn getYarn(String pluginName) {
        return getYarn(DataSourceType.getSourceTypeByPluginName(pluginName).getVal());
    }

    /**
     * 获取 k8s Client 客户端
     *
     * @param dataSourceType 数据源类型 val
     * @return k8s Client 客户端
     */
    public static IKubernetes getKubernetes(Integer dataSourceType) {
        if (DeployMode.LOCAL.equals(DEPLOY_MODE)) {
            return RealClientCache.getKubernetes(dataSourceType);
        }
        return applicationContext.getBean(IKubernetes.class);
    }

    /**
     * 获取 k8s Client 客户端
     *
     * @param pluginName 插件包目录名称
     * @return k8s Client 客户端
     */
    public static IKubernetes getKubernetes(String pluginName) {
        return getKubernetes(DataSourceType.getSourceTypeByPluginName(pluginName).getVal());
    }

    public static void setApplicationContext(ApplicationContext applicationContext) {
        if (ClientCache.applicationContext == null) {
            ClientCache.applicationContext = applicationContext;
        }
    }
}
