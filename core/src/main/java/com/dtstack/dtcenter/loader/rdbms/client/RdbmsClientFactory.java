package com.dtstack.dtcenter.loader.rdbms.client;

import com.dtstack.dtcenter.loader.ClassLoaderCallBack;
import com.dtstack.dtcenter.loader.ClassLoaderCallBackMethod;
import com.dtstack.dtcenter.loader.enums.DataBaseType;
import com.dtstack.dtcenter.loader.service.IRdbmsClient;
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
public class RdbmsClientFactory {
    private static Map<DataBaseType, ClassLoader> pluginClassLoader = Maps.newConcurrentMap();

    public static ClassLoader getClassLoader(DataBaseType dataBaseType) {
        return pluginClassLoader.get(dataBaseType);
    }

    public static IRdbmsClient createPluginClass(DataBaseType dataBaseType) throws Exception {
        ClassLoader classLoader = pluginClassLoader.get(dataBaseType);
        return ClassLoaderCallBackMethod.callbackAndReset(new ClassLoaderCallBack<IRdbmsClient>() {

            @Override
            public IRdbmsClient execute() throws Exception {
                ServiceLoader<IRdbmsClient> iClients = ServiceLoader.load(IRdbmsClient.class);
                Iterator<IRdbmsClient> iClientIterator = iClients.iterator();
                if (!iClientIterator.hasNext()) {
                    throw new RuntimeException("not support for source type " + dataBaseType.name());
                }

                IRdbmsClient client = iClientIterator.next();
                return new RdbmsClientProxy(client);
            }
        }, classLoader, false);
    }

    public static void addClassLoader(DataBaseType dataBaseType, ClassLoader classLoader) {
        if (pluginClassLoader.containsKey(dataBaseType)) {
            return;
        }

        pluginClassLoader.putIfAbsent(dataBaseType, classLoader);
    }

    public static boolean checkContainClassLoader(DataBaseType dataBaseType) {
        return pluginClassLoader.containsKey(dataBaseType);
    }
}
