package com.dtstack.dtcenter.loader.client.hdfs.mq;

import com.dtstack.dtcenter.common.thread.RdosThreadFactory;
import com.dtstack.dtcenter.loader.client.AbsClientCache;
import com.dtstack.dtcenter.loader.client.IClient;
import com.dtstack.dtcenter.loader.client.IHdfsFile;
import com.dtstack.dtcenter.loader.client.IKafka;
import com.dtstack.dtcenter.loader.client.sql.DataSourceClientFactory;
import com.dtstack.dtcenter.loader.exception.ClientAccessException;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.google.common.collect.Maps;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 14:38 2020/8/10
 * @Description： HDFS 文件操作插件客户端
 */
@Slf4j
public class HdfsFileClientCache extends AbsClientCache {

    private Map<String, IHdfsFile> defaultClientMap = Maps.newConcurrentMap();

    private static final ScheduledExecutorService sourceClientExecutor = new ScheduledThreadPoolExecutor(1,
            new RdosThreadFactory("kafkaClientCache"));

    static {
        sourceClientExecutor.scheduleAtFixedRate(new HdfsFileClientCache.CacheTimerTask(), 10, 10,
                TimeUnit.MINUTES);
    }

    static class CacheTimerTask implements Runnable {
        @Override
        public void run() {
            // 处理是否校验插件，如果不校验直接返回
            if (!checkFile.get()) {
                return;
            }

            log.info("开始校验插件是否改变 -- {}", System.currentTimeMillis());
            Iterator<String> iterator = getInstance().pluginMd5.keySet().iterator();
            while (iterator.hasNext()) {
                validateMd5(iterator.next());
            }
            log.info("校验插件是否改变结束 -- {}", System.currentTimeMillis());
        }

        private void validateMd5(String pluginName) {
            try {
                File file = getFileByPluginName(pluginName);
                String oldMd5 = getInstance().pluginMd5.get(pluginName);
                String loaderMd5 = getClassLoaderMd5(file);

                // 如果不空并且与当前值不同
                if (StringUtils.isNotBlank(oldMd5) && StringUtils.isNotBlank(loaderMd5) && !oldMd5.equals(loaderMd5)) {
                    IHdfsFile hdfsFile = getInstance().buildPluginClient(pluginName);
                    getInstance().replaceSourceClient(pluginName, hdfsFile);
                }
            } catch (Exception e) {
                log.error("校验异常，保持原先数据源类型", e);
            }
        }
    }

    private static HdfsFileClientCache singleton = new HdfsFileClientCache();

    private HdfsFileClientCache() {
    }

    public static HdfsFileClientCache getInstance() {
        return singleton;
    }

    @Override
    public IHdfsFile getHdfs(String pluginName) throws ClientAccessException {
        try {
            IHdfsFile hdfsFile = defaultClientMap.get(pluginName);
            if (hdfsFile == null) {
                synchronized (defaultClientMap) {
                    hdfsFile = defaultClientMap.get(pluginName);
                    if (hdfsFile == null) {
                        hdfsFile = buildPluginClient(pluginName);
                        defaultClientMap.put(pluginName, hdfsFile);
                    }
                }
            }

            return hdfsFile;
        } catch (Throwable e) {
            throw new ClientAccessException(e);
        }
    }

    /**
     * 替换过期的 Client 信息
     *
     * @param pluginName
     */
    public void replaceSourceClient(String pluginName, IHdfsFile hdfsFile) {
        DataSourceClientFactory.removePlugin(pluginName);
        getInstance().defaultClientMap.put(pluginName, hdfsFile);
    }


    private IHdfsFile buildPluginClient(String pluginName) throws Exception {
        loadComputerPlugin(pluginName);
        return HdfsFileClientFactory.createPluginClass(pluginName);
    }

    /**
     * 加载 ClassLoader 到 数据源插件工厂
     *
     * @param pluginName
     * @throws Exception
     */
    private void loadComputerPlugin(String pluginName) throws Exception {
        if (HdfsFileClientFactory.checkContainClassLoader(pluginName)) {
            return;
        }

        HdfsFileClientFactory.addClassLoader(pluginName, getClassLoad(pluginName, getFileByPluginName(pluginName)));
    }

    /**
     * 根据插件名称获取文件
     *
     * @param pluginName
     * @return
     * @throws Exception
     */
    @NotNull
    private static File getFileByPluginName(String pluginName) throws Exception {
        String plugin = String.format("%s/%s", userDir, pluginName).replaceAll("//*", "/");
        File finput = new File(plugin);
        if (!finput.exists()) {
            throw new Exception(String.format("%s directory not found", plugin));
        }
        return finput;
    }

    @Override
    public IClient getClient(String sourceName) throws ClientAccessException {
        throw new DtLoaderException("请通过 DataSourceClientCache 获取其他数据库客户端");
    }

    @Override
    public IKafka getKafka(String sourceName) throws ClientAccessException {
        throw  new DtLoaderException("请通过 kafkaClientCache 获取 kafka 客户端");
    }
}
