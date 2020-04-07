package com.dtstack.dtcenter.loader.client.sql;

import com.dtstack.dtcenter.common.thread.RdosThreadFactory;
import com.dtstack.dtcenter.loader.client.AbsClientCache;
import com.dtstack.dtcenter.loader.client.IClient;
import com.dtstack.dtcenter.loader.exception.ClientAccessException;
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
 * @Date ：Created in 15:40 2020/1/6
 * @Description：关系型数据库插件客户端
 */
@Slf4j
public class DataSourceClientCache extends AbsClientCache {
    private Map<String, IClient> defaultClientMap = Maps.newConcurrentMap();

    private static final ScheduledExecutorService sourceClientExecutor = new ScheduledThreadPoolExecutor(1,
            new RdosThreadFactory("sourceClientCache"));

    static {
        sourceClientExecutor.scheduleAtFixedRate(new DataSourceClientCache.CacheTimerTask(), 10, 10,
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
                    IClient client = getInstance().buildPluginClient(pluginName);
                    getInstance().replaceSourceClient(pluginName, client);
                }
            } catch (Exception e) {
                log.error("校验异常，保持原先数据源类型", e);
            }
        }
    }

    private static DataSourceClientCache singleton = new DataSourceClientCache();

    private DataSourceClientCache() {
    }

    public static DataSourceClientCache getInstance() {
        return singleton;
    }

    /**
     * 替换过期的 Client 信息
     *
     * @param pluginName
     */
    public void replaceSourceClient(String pluginName, IClient client) {
        DataSourceClientFactory.removePlugin(pluginName);
        getInstance().defaultClientMap.put(pluginName, client);
    }

    /**
     * 根据数据源类型获取对应的客户端
     *
     * @param pluginName
     * @return
     */
    @Override
    public IClient getClient(String pluginName) throws ClientAccessException {
        try {
            IClient client = defaultClientMap.get(pluginName);
            if (client == null) {
                synchronized (defaultClientMap) {
                    client = buildPluginClient(pluginName);
                    defaultClientMap.put(pluginName, client);
                }
            }

            return client;
        } catch (Throwable e) {
            throw new ClientAccessException(e);
        }
    }

    private IClient buildPluginClient(String pluginName) throws Exception {
        loadComputerPlugin(pluginName);
        return DataSourceClientFactory.createPluginClass(pluginName);
    }

    /**
     * 加载 ClassLoader 到 数据源插件工厂
     *
     * @param pluginName
     * @throws Exception
     */
    private void loadComputerPlugin(String pluginName) throws Exception {
        if (DataSourceClientFactory.checkContainClassLoader(pluginName)) {
            return;
        }

        DataSourceClientFactory.addClassLoader(pluginName, getClassLoad(pluginName, getFileByPluginName(pluginName)));
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
        String plugin = String.format("%s/%s", userDir, pluginName);
        File finput = new File(plugin);
        if (!finput.exists()) {
            throw new Exception(String.format("%s directory not found", plugin));
        }
        return finput;
    }
}
