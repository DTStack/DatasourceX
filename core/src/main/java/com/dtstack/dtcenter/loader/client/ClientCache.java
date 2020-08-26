package com.dtstack.dtcenter.loader.client;

import com.dtstack.dtcenter.loader.client.hdfs.HdfsFileClientFactory;
import com.dtstack.dtcenter.loader.client.kerberos.KerberosClientFactory;
import com.dtstack.dtcenter.loader.client.mq.KafkaClientFactory;
import com.dtstack.dtcenter.loader.client.sql.DataSourceClientFactory;
import com.dtstack.dtcenter.loader.exception.ClientAccessException;
import com.google.common.collect.Maps;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 10:18 2020/1/13
 * 如果需要开启插件校验，请使用 startCheckFile 来校验文件
 * @Description：抽象客户端缓存
 */
@Slf4j
public class ClientCache {
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
     * Kerberos 认证服务
     */
    private static IKerberos kerberos;

    static {
        try {
            kerberos = KerberosClientFactory.createPluginClass();
        } catch (Exception e) {
            log.error("初始化 Kerberos 配置异常 : {}",  e.getMessage(), e);
        }
    }

    protected static String userDir = String.format("%s/pluginLibs/", System.getProperty("user.dir"));

    /**
     * 修改插件包文件夹路径
     *
     * @param dir
     */
    public static void setUserDir(String dir) {
        ClientCache.userDir = dir;
    }

    /**
     * 获取插件包文件夹路径
     *
     * @return
     */
    public static String getUserDir() {
        return userDir;
    }

    /**
     * 获取 Sql Client 客户端
     *
     * @param pluginName
     * @return
     * @throws ClientAccessException
     */
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
     * @param pluginName
     * @return
     * @throws ClientAccessException
     */
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
     * @param pluginName
     * @return
     * @throws ClientAccessException
     */
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

    /**
     * 获取 Kerberos 服务客户端
     *
     * @return
     * @throws ClientAccessException
     */
    public static IKerberos getKerberos() throws ClientAccessException {
        return kerberos;
    }
}
