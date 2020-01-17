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
    private static Map<DataSourceType, ClassLoader> pluginClassLoader = Maps.newConcurrentMap();

    public static ClassLoader getClassLoader(DataSourceType dataBaseType) {
        return pluginClassLoader.get(dataBaseType);
    }

    public static IClient createPluginClass(DataSourceType dataBaseType) throws Exception {
        ClassLoader classLoader = pluginClassLoader.get(dataBaseType);
        return ClassLoaderCallBackMethod.callbackAndReset(new ClassLoaderCallBack<IClient>() {

            @Override
            public IClient execute() throws Exception {
                ServiceLoader<IClient> iClients = ServiceLoader.load(IClient.class);
                Iterator<IClient> iClientIterator = iClients.iterator();
                if (!iClientIterator.hasNext()) {
                    throw new RuntimeException("not support for source type " + dataBaseType.name());
                }

                IClient client = iClientIterator.next();
                return new DataSourceClientProxy(client);
            }
        }, classLoader, false);
    }

    public static void addClassLoader(DataSourceType sourceType, ClassLoader classLoader) {
        if (pluginClassLoader.containsKey(sourceType)) {
            return;
        }

        pluginClassLoader.putIfAbsent(sourceType, classLoader);
    }

    public static boolean checkContainClassLoader(DataSourceType sourceType) {
        return pluginClassLoader.containsKey(sourceType);
    }
}
