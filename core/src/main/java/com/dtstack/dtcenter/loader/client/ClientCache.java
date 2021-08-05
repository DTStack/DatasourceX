package com.dtstack.dtcenter.loader.client;

import com.dtstack.dtcenter.loader.client.hbase.HbaseClientFactory;
import com.dtstack.dtcenter.loader.client.hdfs.HdfsFileClientFactory;
import com.dtstack.dtcenter.loader.client.kerberos.KerberosClientFactory;
import com.dtstack.dtcenter.loader.client.mq.KafkaClientFactory;
import com.dtstack.dtcenter.loader.client.sql.DataSourceClientFactory;
import com.dtstack.dtcenter.loader.client.table.TableClientFactory;
import com.dtstack.dtcenter.loader.client.tsdb.TsdbClientFactory;
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
                    kerberos = KERBEROS_CLIENT.get(pluginName);
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
                    hbase = HBASE_CLIENT.get(pluginName);
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
                    table = TABLE_CLIENT.get(pluginName);
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
                    tsdb = TSDB_CLIENT.get(pluginName);
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

}
