package com.dtstack.dtcenter.loader.client.mq;

import com.dtstack.dtcenter.common.thread.RdosThreadFactory;
import com.dtstack.dtcenter.loader.client.AbsClientCache;
import com.dtstack.dtcenter.loader.client.IClient;
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
 * @Author ：wangchuan
 * @Date ：Created in 下午2:14 2020/6/2
 * @Description： kafka 插件客户端
 */
@Slf4j
public class KafkaClientCache extends AbsClientCache {

    private Map<String, IKafka> defaultClientMap = Maps.newConcurrentMap();

    private static final ScheduledExecutorService sourceClientExecutor = new ScheduledThreadPoolExecutor(1,
            new RdosThreadFactory("kafkaClientCache"));

    static {
        sourceClientExecutor.scheduleAtFixedRate(new KafkaClientCache.CacheTimerTask(), 10, 10,
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
                    IKafka kafka = getInstance().buildPluginClient(pluginName);
                    getInstance().replaceSourceClient(pluginName, kafka);
                }
            } catch (Exception e) {
                log.error("校验异常，保持原先数据源类型", e);
            }
        }
    }

    private static KafkaClientCache singleton = new KafkaClientCache();

    private KafkaClientCache() {
    }

    public static KafkaClientCache getInstance() {
        return singleton;
    }

    /**
     * 替换过期的 Client 信息
     *
     * @param pluginName
     */
    public void replaceSourceClient(String pluginName, IKafka kafka) {
        DataSourceClientFactory.removePlugin(pluginName);
        getInstance().defaultClientMap.put(pluginName, kafka);
    }

    /**
     * 根据数据源类型获取对应的客户端
     *
     * @param pluginName
     * @return
     */
    @Override
    public IKafka getKafka(String pluginName) throws ClientAccessException {
        try {
            IKafka kafka = defaultClientMap.get(pluginName);
            if (kafka == null) {
                synchronized (defaultClientMap) {
                    kafka = buildPluginClient(pluginName);
                    defaultClientMap.put(pluginName, kafka);
                }
            }

            return kafka;
        } catch (Throwable e) {
            throw new ClientAccessException(e);
        }
    }

    private IKafka buildPluginClient(String pluginName) throws Exception {
        loadComputerPlugin(pluginName);
        return KafkaClientFactory.createPluginClass(pluginName);
    }

    /**
     * 加载 ClassLoader 到 数据源插件工厂
     *
     * @param pluginName
     * @throws Exception
     */
    private void loadComputerPlugin(String pluginName) throws Exception {
        if (KafkaClientFactory.checkContainClassLoader(pluginName)) {
            return;
        }

        KafkaClientFactory.addClassLoader(pluginName, getClassLoad(pluginName, getFileByPluginName(pluginName)));
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
        throw new DtLoaderException("请通过DataSourceClientCache获取其他数据库客户端");
    }
}
