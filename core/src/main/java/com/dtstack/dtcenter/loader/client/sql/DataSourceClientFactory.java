package com.dtstack.dtcenter.loader.client.sql;

import com.dtstack.dtcenter.common.enums.DataSourceType;
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
    private static Map<Integer, ClassLoader> pluginClassLoader = Maps.newConcurrentMap();

    public static IClient createPluginClass(DataSourceType source) throws Exception {
        ClassLoader classLoader = pluginClassLoader.get(source.getVal());
        return ClassLoaderCallBackMethod.callbackAndReset(new ClassLoaderCallBack<IClient>() {

            @Override
            public IClient execute() throws Exception {
                ServiceLoader<IClient> iClients = ServiceLoader.load(IClient.class);
                Iterator<IClient> iClientIterator = iClients.iterator();
                if (!iClientIterator.hasNext()) {
                    throw new RuntimeException("not support for source type " + source.name());
                }

                IClient client = iClientIterator.next();
                return new DataSourceClientProxy(client);
            }
        }, classLoader, false);
    }

    public static void addClassLoader(DataSourceType source, ClassLoader classLoader) {
        if (pluginClassLoader.containsKey(source.getVal())) {
            return;
        }

        pluginClassLoader.putIfAbsent(source.getVal(), classLoader);
    }

    public static boolean checkContainClassLoader(DataSourceType source) {
        return pluginClassLoader.containsKey(source.getVal());
    }
}
