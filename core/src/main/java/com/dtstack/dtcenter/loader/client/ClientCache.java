package com.dtstack.dtcenter.loader.client;

import com.dtstack.dtcenter.loader.client.hdfs.HdfsFileClientFactory;
import com.dtstack.dtcenter.loader.client.kerberos.KerberosClientFactory;
import com.dtstack.dtcenter.loader.client.mq.KafkaClientFactory;
import com.dtstack.dtcenter.loader.client.sql.DataSourceClientFactory;
import com.dtstack.dtcenter.loader.exception.ClientAccessException;
import com.dtstack.dtcenter.loader.source.DataSourceType;
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
     * Kerberos 认证服务客户端缓存
     */
    private static final Map<String, IKerberos> KERBEROS_CLIENT = Maps.newConcurrentMap();

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
     * @param sourceType
     * @return
     * @throws ClientAccessException
     */
    public static IClient getClient(Integer sourceType) throws ClientAccessException {
        try {
            String pluginName = DataSourceType.getSourceType(sourceType).getPluginName();
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
        try {
            String pluginName = DataSourceType.getSourceType(sourceType).getPluginName();
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
        try {
            String pluginName = DataSourceType.getSourceType(sourceType).getPluginName();
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
     * @param sourceType
     * @return
     * @throws ClientAccessException
     */
    public static IKerberos getKerberos(Integer sourceType) throws ClientAccessException {
        try {
            String pluginName = DataSourceType.getSourceType(sourceType).getPluginName();
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
}
