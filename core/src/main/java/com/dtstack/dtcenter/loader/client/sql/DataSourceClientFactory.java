package com.dtstack.dtcenter.loader.client.sql;

import com.dtstack.dtcenter.loader.ClassLoaderCallBack;
import com.dtstack.dtcenter.loader.ClassLoaderCallBackMethod;
import com.dtstack.dtcenter.loader.client.IClient;
import com.google.common.collect.Maps;

import java.util.Iterator;
import java.util.Map;
import java.util.ServiceLoader;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 15:50 2020/1/6
 * @Description：关系型数据库客户端工厂
 */
public class DataSourceClientFactory {
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

    public static IClient createPluginClass(String pluginName) throws Exception {
        ClassLoader classLoader = pluginClassLoader.get(pluginName);
        return ClassLoaderCallBackMethod.callbackAndReset((ClassLoaderCallBack<IClient>) () -> {
            ServiceLoader<IClient> iClients = ServiceLoader.load(IClient.class);
            Iterator<IClient> iClientIterator = iClients.iterator();
            if (!iClientIterator.hasNext()) {
                throw new RuntimeException("暂不支持该插件类型: " + pluginName);
            }

            IClient client = iClientIterator.next();
            return new DataSourceClientProxy(client);
        }, classLoader, true);
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
