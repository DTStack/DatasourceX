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

package com.dtstack.dtcenter.loader.cache.client;

import com.dtstack.dtcenter.loader.cache.client.job.JobClientFactory;
import com.dtstack.dtcenter.loader.cache.client.kubernetes.KubernetesClientFactory;
import com.dtstack.dtcenter.loader.cache.client.mq.RabbitMqClientFactory;
import com.dtstack.dtcenter.loader.cache.client.mq.RocketMqClientFactory;
import com.dtstack.dtcenter.loader.cache.client.yarn.YarnClientFactory;
import com.dtstack.dtcenter.loader.client.IClient;
import com.dtstack.dtcenter.loader.client.IHbase;
import com.dtstack.dtcenter.loader.client.IHdfsFile;
import com.dtstack.dtcenter.loader.client.IJob;
import com.dtstack.dtcenter.loader.client.IKafka;
import com.dtstack.dtcenter.loader.client.IKerberos;
import com.dtstack.dtcenter.loader.client.IKubernetes;
import com.dtstack.dtcenter.loader.client.IRabbitMq;
import com.dtstack.dtcenter.loader.client.IRedis;
import com.dtstack.dtcenter.loader.client.IRestful;
import com.dtstack.dtcenter.loader.client.IRocketMq;
import com.dtstack.dtcenter.loader.client.ITable;
import com.dtstack.dtcenter.loader.client.ITsdb;
import com.dtstack.dtcenter.loader.client.IYarn;
import com.dtstack.dtcenter.loader.exception.ClientAccessException;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import com.dtstack.dtcenter.loader.cache.client.hbase.HbaseClientFactory;
import com.dtstack.dtcenter.loader.cache.client.hdfs.HdfsFileClientFactory;
import com.dtstack.dtcenter.loader.cache.client.kerberos.KerberosClientFactory;
import com.dtstack.dtcenter.loader.cache.client.mq.KafkaClientFactory;
import com.dtstack.dtcenter.loader.cache.client.redis.RedisClientFactory;
import com.dtstack.dtcenter.loader.cache.client.restful.RestfulClientFactory;
import com.dtstack.dtcenter.loader.cache.client.sql.DataSourceClientFactory;
import com.dtstack.dtcenter.loader.cache.client.table.TableClientFactory;
import com.dtstack.dtcenter.loader.cache.client.tsdb.TsdbClientFactory;
import com.google.common.collect.Maps;

import java.util.Map;

/**
 * real client cache
 *
 * @author ：wangchuan
 * date：Created in 下午5:29 2021/12/17
 * company: www.dtstack.com
 */
public class RealClientCache {

    /**
     * Sql Client 客户端缓存
     */
    private static final Map<String, IClient> SQL_CLIENT = Maps.newConcurrentMap();

    /**
     * HDFS 文件客户端缓存
     */
    private static final Map<String, IHdfsFile> HDFS_FILE_CLIENT = Maps.newConcurrentMap();

    /**
     * KAFKA 客户端缓存
     */
    private static final Map<String, IKafka> KAFKA_CLIENT = Maps.newConcurrentMap();

    /**
     * RocketMq 客户端缓存
     */
    private static final Map<String, IRocketMq> ROCKET_MQ_CLIENT = Maps.newConcurrentMap();


    /**
     * RabbitMq 客户端缓存
     */
    private static final Map<String, IRabbitMq> RABBIT_MQ_CLIENT = Maps.newConcurrentMap();

    /**
     * Kerberos 认证服务客户端缓存
     */
    private static final Map<String, IKerberos> KERBEROS_CLIENT = Maps.newConcurrentMap();

    /**
     * hbase 服务客户端缓存
     */
    private static final Map<String, IHbase> HBASE_CLIENT = Maps.newConcurrentMap();

    /**
     * table 客户端缓存
     */
    private static final Map<String, ITable> TABLE_CLIENT = Maps.newConcurrentMap();

    /**
     * tsdb 客户端缓存
     */
    private static final Map<String, ITsdb> TSDB_CLIENT = Maps.newConcurrentMap();

    /**
     * restful 客户端缓存
     */
    private static final Map<String, IRestful> RESTFUL_CLIENT = Maps.newConcurrentMap();

    /**
     * redis 客户端缓存
     */
    private static final Map<String, IRedis> REDIS_CLIENT = Maps.newConcurrentMap();

    /**
     * job 客户端缓存
     */
    private static final Map<String, IJob> JOB_CLIENT = Maps.newConcurrentMap();

    /**
     * yarn 客户端缓存
     */
    private static final Map<String, IYarn> YARN_CLIENT = Maps.newConcurrentMap();

    /**
     * k8s 客户端缓存
     */
    private static final Map<String, IKubernetes> KUBERNETES_CLIENT = Maps.newConcurrentMap();

    protected static String pluginDir = String.format("%s/pluginLibs/", System.getProperty("user.dir"));

    /**
     * 修改插件包文件夹路径
     *
     * @param pluginDir 插件包路径
     */
    public static void setUserDir(String pluginDir) {
        RealClientCache.pluginDir = pluginDir;
    }

    /**
     * 获取插件包文件夹路径
     *
     * @return 插件包路径
     */
    public static String getUserDir() {
        return pluginDir;
    }

    /**
     * 获取 Sql Client 客户端
     *
     * @param sourceType 数据源类型
     * @return Client 客户端
     * @throws ClientAccessException 插件初始化异常
     */
    public static IClient getClient(Integer sourceType) throws ClientAccessException {
        String pluginName = DataSourceType.getSourceType(sourceType).getPluginName();
        return getClient(pluginName);
    }

    /**
     * 获取 Sql Client 客户端
     *
     * @param pluginName
     * @return
     * @throws ClientAccessException
     */
    @Deprecated
    public static IClient getClient(String pluginName) throws ClientAccessException {
        try {
            IClient client = SQL_CLIENT.get(pluginName);
            if (client == null) {
                synchronized (SQL_CLIENT) {
                    client = SQL_CLIENT.get(pluginName);
                    if (client == null) {
                        client = DataSourceClientFactory.createPluginClass(pluginName);
                        SQL_CLIENT.put(pluginName, client);
                    }
                }
            }

            return client;
        } catch (Throwable e) {
            throw new ClientAccessException(e);
        }
    }

    /**
     * 获取 HDFS 文件客户端
     *
     * @param sourceType
     * @return
     * @throws ClientAccessException
     */
    public static IHdfsFile getHdfs(Integer sourceType) throws ClientAccessException {
        String pluginName = DataSourceType.getSourceType(sourceType).getPluginName();
        return getHdfs(pluginName);
    }

    /**
     * 获取 HDFS 文件客户端
     *
     * @param pluginName
     * @return
     * @throws ClientAccessException
     */
    @Deprecated
    public static IHdfsFile getHdfs(String pluginName) throws ClientAccessException {
        try {
            IHdfsFile hdfsFile = HDFS_FILE_CLIENT.get(pluginName);
            if (hdfsFile == null) {
                synchronized (HDFS_FILE_CLIENT) {
                    hdfsFile = HDFS_FILE_CLIENT.get(pluginName);
                    if (hdfsFile == null) {
                        hdfsFile = HdfsFileClientFactory.createPluginClass(pluginName);
                        HDFS_FILE_CLIENT.put(pluginName, hdfsFile);
                    }
                }
            }

            return hdfsFile;
        } catch (Throwable e) {
            throw new ClientAccessException(e);
        }
    }

    /**
     * 获取 KAFKA 客户端
     *
     * @param sourceType
     * @return
     * @throws ClientAccessException
     */
    public static IKafka getKafka(Integer sourceType) throws ClientAccessException {
        String pluginName = DataSourceType.getSourceType(sourceType).getPluginName();
        return getKafka(pluginName);
    }


    /**
     * 获取 ROCKET MQ 客户端
     *
     * @param sourceType
     * @return
     * @throws ClientAccessException
     */
    public static IRocketMq getRocketMq(Integer sourceType) {
        String pluginName = DataSourceType.getSourceType(sourceType).getPluginName();
        return getRocketMq(pluginName);
    }

    /**
     * 获取 Rabbit MQ 客户端
     *
     * @param sourceType
     * @return
     * @throws ClientAccessException
     */
    public static IRabbitMq getRabbitMq(Integer sourceType) {
        String pluginName = DataSourceType.getSourceType(sourceType).getPluginName();
        return getRabbitMq(pluginName);
    }


    /**
     * 获取 KAFKA 客户端
     *
     * @param pluginName
     * @return
     * @throws ClientAccessException
     */
    @Deprecated
    public static IKafka getKafka(String pluginName) throws ClientAccessException {
        try {
            IKafka kafka = KAFKA_CLIENT.get(pluginName);
            if (kafka == null) {
                synchronized (KAFKA_CLIENT) {
                    kafka = KAFKA_CLIENT.get(pluginName);
                    if (kafka == null) {
                        kafka = KafkaClientFactory.createPluginClass(pluginName);
                        KAFKA_CLIENT.put(pluginName, kafka);
                    }
                }
            }

            return kafka;
        } catch (Throwable e) {
            throw new ClientAccessException(e);
        }
    }

    public static IRocketMq getRocketMq(String pluginName) throws ClientAccessException {
        try {
            IRocketMq rocketMq = ROCKET_MQ_CLIENT.get(pluginName);
            if (rocketMq == null) {
                synchronized (ROCKET_MQ_CLIENT) {
                    rocketMq = ROCKET_MQ_CLIENT.get(pluginName);
                    if (rocketMq == null) {
                        rocketMq = RocketMqClientFactory.createPluginClass(pluginName);
                        ROCKET_MQ_CLIENT.put(pluginName, rocketMq);
                    }
                }
            }

            return rocketMq;
        } catch (Throwable e) {
            throw new ClientAccessException(e);
        }
    }


    public static IRabbitMq getRabbitMq(String pluginName) throws ClientAccessException {
        try {
            IRabbitMq rabbitMq = RABBIT_MQ_CLIENT.get(pluginName);
            if (rabbitMq == null) {
                synchronized (RABBIT_MQ_CLIENT) {
                    rabbitMq = RABBIT_MQ_CLIENT.get(pluginName);
                    if (rabbitMq == null) {
                        rabbitMq = RabbitMqClientFactory.createPluginClass(pluginName);
                        RABBIT_MQ_CLIENT.put(pluginName, rabbitMq);
                    }
                }
            }

            return rabbitMq;
        } catch (Throwable e) {
            throw new ClientAccessException(e);
        }
    }

    /**
     * 获取 Kerberos 服务客户端
     *
     * @param sourceType
     * @return
     * @throws ClientAccessException
     */
    public static IKerberos getKerberos(Integer sourceType) throws ClientAccessException {
        String pluginName = DataSourceType.getSourceType(sourceType).getPluginName();
        return getKerberos(pluginName);
    }

    /**
     * 获取 Kerberos 服务客户端
     *
     * @param pluginName
     * @return
     * @throws ClientAccessException
     */
    private static IKerberos getKerberos(String pluginName) throws ClientAccessException {
        try {
            IKerberos kerberos = KERBEROS_CLIENT.get(pluginName);
            if (kerberos == null) {
                synchronized (KERBEROS_CLIENT) {
                    if (kerberos == null) {
                        kerberos = KerberosClientFactory.createPluginClass(pluginName);
                        KERBEROS_CLIENT.put(pluginName, kerberos);
                    }
                }
            }

            return kerberos;
        } catch (Throwable e) {
            throw new ClientAccessException(e);
        }
    }

    /**
     * 获取 hbase 服务客户端
     *
     * @param sourceType 数据源类型
     * @return hbase新客户端
     * @throws ClientAccessException 插件化加载异常
     */
    public static IHbase getHbase(Integer sourceType) throws ClientAccessException {
        String pluginName = DataSourceType.getSourceType(sourceType).getPluginName();
        return getHbase(pluginName);
    }

    /**
     * 获取 hbase 服务客户端
     *
     * @param pluginName 数据源插件包名称
     * @return hbase新客户端
     * @throws ClientAccessException 插件化加载异常
     */
    private static IHbase getHbase(String pluginName) throws ClientAccessException {
        try {
            IHbase hbase = HBASE_CLIENT.get(pluginName);
            if (hbase == null) {
                synchronized (HBASE_CLIENT) {
                    if (hbase == null) {
                        hbase = HbaseClientFactory.createPluginClass(pluginName);
                        HBASE_CLIENT.put(pluginName, hbase);
                    }
                }
            }
            return hbase;
        } catch (Throwable e) {
            throw new ClientAccessException(e);
        }
    }

    /**
     * 获取 table Client 客户端
     *
     * @param sourceType
     * @return
     * @throws ClientAccessException
     */
    public static ITable getTable(Integer sourceType) throws ClientAccessException {
        String pluginName = DataSourceType.getSourceType(sourceType).getPluginName();
        return getTable(pluginName);
    }

    private static ITable getTable(String pluginName) {
        try {
            ITable table = TABLE_CLIENT.get(pluginName);
            if (table == null) {
                synchronized (TABLE_CLIENT) {
                    if (table == null) {
                        table = TableClientFactory.createPluginClass(pluginName);
                        TABLE_CLIENT.put(pluginName, table);
                    }
                }
            }

            return table;
        } catch (Throwable e) {
            throw new ClientAccessException(e);
        }
    }

    /**
     * 获取 tsdb Client 客户端
     *
     * @param sourceType 数据源类型
     * @return tsdb Client 客户端
     */
    public static ITsdb getTsdb(Integer sourceType) {
        String pluginName = DataSourceType.getSourceType(sourceType).getPluginName();
        return getTsdb(pluginName);
    }

    private static ITsdb getTsdb(String pluginName) {
        try {
            ITsdb tsdb = TSDB_CLIENT.get(pluginName);
            if (tsdb == null) {
                synchronized (TSDB_CLIENT) {
                    if (tsdb == null) {
                        tsdb = TsdbClientFactory.createPluginClass(pluginName);
                        TSDB_CLIENT.put(pluginName, tsdb);
                    }
                }
            }

            return tsdb;
        } catch (Throwable e) {
            throw new ClientAccessException(e);
        }
    }

    /**
     * 获取 restful Client 客户端
     *
     * @param sourceType 数据源类型
     * @return restful Client 客户端
     */
    public static IRestful getRestful(Integer sourceType) {
        String pluginName = DataSourceType.getSourceType(sourceType).getPluginName();
        return getRestful(pluginName);
    }

    /**
     * 获取 restful Client 客户端
     *
     * @param sourceType 数据源类型
     * @return restful Client 客户端
     */
    public static IRedis getRedis(Integer sourceType) {
        String pluginName = DataSourceType.getSourceType(sourceType).getPluginName();
        return getRedis(pluginName);
    }

    /**
     * 获取 restful Client 客户端
     *
     * @param pluginName 数据源插件包名
     * @return restful Client 客户端
     */
    private static IRedis getRedis(String pluginName) {
        try {
            IRedis redis = REDIS_CLIENT.get(pluginName);
            if (redis == null) {
                synchronized (REDIS_CLIENT) {
                    redis = REDIS_CLIENT.get(pluginName);
                    if (redis == null) {
                        redis = RedisClientFactory.createPluginClass(pluginName);
                        REDIS_CLIENT.put(pluginName, redis);
                    }
                }
            }
            return redis;
        } catch (Throwable e) {
            throw new ClientAccessException(e);
        }
    }

    private static IRestful getRestful(String pluginName) {
        try {
            IRestful restful = RESTFUL_CLIENT.get(pluginName);
            if (restful == null) {
                synchronized (RESTFUL_CLIENT) {
                    restful = RESTFUL_CLIENT.get(pluginName);
                    if (restful == null) {
                        restful = RestfulClientFactory.createPluginClass(pluginName);
                        RESTFUL_CLIENT.put(pluginName, restful);
                    }
                }
            }
            return restful;
        } catch (Throwable e) {
            throw new ClientAccessException(e);
        }
    }


    /**
     * 获取 job Client 客户端
     *
     * @param sourceType 数据源类型
     * @return job Client 客户端
     */
    public static IJob getJob(Integer sourceType) {
        String pluginName = DataSourceType.getSourceType(sourceType).getPluginName();
        return getJob(pluginName);
    }

    private static IJob getJob(String pluginName) {
        try {
            IJob job = JOB_CLIENT.get(pluginName);
            if (job == null) {
                synchronized (JOB_CLIENT) {
                    job = JOB_CLIENT.get(pluginName);
                    if (job == null) {
                        job = JobClientFactory.createPluginClass(pluginName);
                        JOB_CLIENT.put(pluginName, job);
                    }
                }
            }
            return job;
        } catch (Throwable e) {
            throw new ClientAccessException(e);
        }
    }

    /**
     * 获取 yarn Client 客户端
     *
     * @param sourceType 数据源类型
     * @return yarn Client 客户端
     */
    public static IYarn getYarn(Integer sourceType) {
        String pluginName = DataSourceType.getSourceType(sourceType).getPluginName();
        return getYarn(pluginName);
    }

    private static IYarn getYarn(String pluginName) {
        try {
            IYarn yarn = YARN_CLIENT.get(pluginName);
            if (yarn == null) {
                synchronized (YARN_CLIENT) {
                    yarn = YARN_CLIENT.get(pluginName);
                    if (yarn == null) {
                        yarn = YarnClientFactory.createPluginClass(pluginName);
                        YARN_CLIENT.put(pluginName, yarn);
                    }
                }
            }
            return yarn;
        } catch (Throwable e) {
            throw new ClientAccessException(e);
        }
    }

    /**
     * 获取 k8s Client 客户端
     *
     * @param sourceType 数据源类型
     * @return kubernetes Client 客户端
     */
    public static IKubernetes getKubernetes(Integer sourceType) {
        String pluginName = DataSourceType.getSourceType(sourceType).getPluginName();
        return getKubernetes(pluginName);
    }

    private static IKubernetes getKubernetes(String pluginName) {
        try {
            IKubernetes kubernetes = KUBERNETES_CLIENT.get(pluginName);
            if (kubernetes == null) {
                synchronized (KUBERNETES_CLIENT) {
                    kubernetes = KUBERNETES_CLIENT.get(pluginName);
                    if (kubernetes == null) {
                        kubernetes = KubernetesClientFactory.createPluginClass(pluginName);
                        KUBERNETES_CLIENT.put(pluginName, kubernetes);
                    }
                }
            }
            return kubernetes;
        } catch (Throwable e) {
            throw new ClientAccessException(e);
        }
    }

    @SuppressWarnings("unchecked")
    public static <T> T getByClass(Class<T> clazz, Integer sourceType) {
        if (IClient.class.isAssignableFrom(clazz)) {
            return (T) RealClientCache.getClient(sourceType);
        } else if (IHbase.class.isAssignableFrom(clazz)) {
            return (T) RealClientCache.getHbase(sourceType);
        } else if (IHdfsFile.class.isAssignableFrom(clazz)) {
            return (T) RealClientCache.getHdfs(sourceType);
        } else if (IKafka.class.isAssignableFrom(clazz)) {
            return (T) RealClientCache.getKafka(sourceType);
        } else if (IKerberos.class.isAssignableFrom(clazz)) {
            return (T) RealClientCache.getKerberos(sourceType);
        } else if (ITable.class.isAssignableFrom(clazz)) {
            return (T) RealClientCache.getTable(sourceType);
        } else if (ITsdb.class.isAssignableFrom(clazz)) {
            return (T) RealClientCache.getTsdb(sourceType);
        } else if (IRestful.class.isAssignableFrom(clazz)) {
            return (T) RealClientCache.getRestful(sourceType);
        } else if (IRedis.class.isAssignableFrom(clazz)) {
            return (T) RealClientCache.getRedis(sourceType);
        } else if (IJob.class.isAssignableFrom(clazz)) {
            return (T) RealClientCache.getJob(sourceType);
        } else if (IYarn.class.isAssignableFrom(clazz)) {
            return (T) RealClientCache.getYarn(sourceType);
        } else if (IKubernetes.class.isAssignableFrom(clazz)) {
            return (T) RealClientCache.getKubernetes(sourceType);
        } else {
            throw new DtLoaderException(String.format("class: %s is not support", clazz.getTypeName()));
        }
    }
}
