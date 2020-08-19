package com.dtstack.dtcenter.loader.client.mq;

import com.dtstack.dtcenter.loader.client.AbsClientCache;
import com.dtstack.dtcenter.loader.client.IKafka;
import com.dtstack.dtcenter.loader.exception.ClientAccessException;
import com.google.common.collect.Maps;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;

/**
 * @company: www.dtstack.com
 * @Author ：wangchuan
 * @Date ：Created in 下午2:14 2020/6/2
 * @Description： kafka 插件客户端
 */
@Slf4j
@NoArgsConstructor
public class KafkaClientCache extends AbsClientCache {

    private Map<String, IKafka> defaultClientMap = Maps.newConcurrentMap();

    private static KafkaClientCache singleton = new KafkaClientCache();

    public static KafkaClientCache getInstance() {
        return singleton;
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
                    kafka = defaultClientMap.get(pluginName);
                    if (kafka == null) {
                        kafka = buildPluginClient(pluginName);
                        defaultClientMap.put(pluginName, kafka);
                    }
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
}
