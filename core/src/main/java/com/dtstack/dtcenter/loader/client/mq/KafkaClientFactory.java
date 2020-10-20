package com.dtstack.dtcenter.loader.client.mq;

import com.dtstack.dtcenter.loader.ClassLoaderCallBack;
import com.dtstack.dtcenter.loader.ClassLoaderCallBackMethod;
import com.dtstack.dtcenter.loader.client.IKafka;
import com.google.common.collect.Maps;

import java.util.Iterator;
import java.util.Map;
import java.util.ServiceLoader;

/**
 * @company: www.dtstack.com
 * @Author ：wangchuan
 * @Date ：Created in 下午2:23 2020/6/2
 * @Description：kafka客户端工厂
 */
public class KafkaClientFactory {

    /**
     * 存储 插件名称 - ClassLoader 键值对信息
     */
    private static Map<String, ClassLoader> pluginClassLoader = Maps.newConcurrentMap();

    /**
     * 去除特定的插件缓存
     *
     * @param pluginName
     */
    public static void removePlugin(String pluginName) {
        pluginClassLoader.remove(pluginName);
    }

    public static IKafka createPluginClass(String pluginName) throws Exception {
        ClassLoader classLoader = pluginClassLoader.get(pluginName);
        return ClassLoaderCallBackMethod.callbackAndReset((ClassLoaderCallBack<IKafka>) () -> {
            ServiceLoader<IKafka> kafkas = ServiceLoader.load(IKafka.class);
            Iterator<IKafka> iClientIterator = kafkas.iterator();
            if (!iClientIterator.hasNext()) {
                throw new RuntimeException("暂不支持该插件类型: " + pluginName);
            }

            IKafka kafka = iClientIterator.next();
            return new KafkaClientProxy(kafka);
        }, classLoader);
    }

    public static void addClassLoader(String pluginName, ClassLoader classLoader) {
        if (pluginClassLoader.containsKey(pluginName)) {
            return;
        }

        pluginClassLoader.put(pluginName, classLoader);
    }

    public static boolean checkContainClassLoader(String pluginName) {
        return pluginClassLoader.containsKey(pluginName);
    }
}
