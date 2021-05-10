package com.dtstack.dtcenter.loader.client.sql;

import com.dtstack.dtcenter.loader.ClassLoaderCallBack;
import com.dtstack.dtcenter.loader.ClassLoaderCallBackMethod;
import com.dtstack.dtcenter.loader.client.ClientFactory;
import com.dtstack.dtcenter.loader.client.IClient;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;

import java.util.Iterator;
import java.util.ServiceLoader;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 15:50 2020/1/6
 * @Description：关系型数据库客户端工厂
 */
public class DataSourceClientFactory {
    public static IClient createPluginClass(String pluginName) throws Exception {
        ClassLoader classLoader = ClientFactory.getClassLoader(pluginName);
        return ClassLoaderCallBackMethod.callbackAndReset((ClassLoaderCallBack<IClient>) () -> {
            ServiceLoader<IClient> iClients = ServiceLoader.load(IClient.class);
            Iterator<IClient> iClientIterator = iClients.iterator();
            if (!iClientIterator.hasNext()) {
                throw new DtLoaderException("This plugin type is not supported: " + pluginName);
            }

            IClient client = iClientIterator.next();
            return new DataSourceClientProxy(client);
        }, classLoader);
    }
}
