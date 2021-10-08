package com.dtstack.dtcenter.loader.client.redis;

import com.dtstack.dtcenter.loader.ClassLoaderCallBack;
import com.dtstack.dtcenter.loader.ClassLoaderCallBackMethod;
import com.dtstack.dtcenter.loader.client.ClientFactory;
import com.dtstack.dtcenter.loader.client.IRedis;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;

import java.util.Iterator;
import java.util.ServiceLoader;

/**
 * <p> redis 客户端工厂</p>
 *
 * @author ：qianyi
 * date：Created in 上午10:06 2021/8/16
 * company: www.dtstack.com
 */
public class RedisClientFactory {
    public static IRedis createPluginClass(String pluginName) throws Exception {
        ClassLoader classLoader = ClientFactory.getClassLoader(pluginName);
        return ClassLoaderCallBackMethod.callbackAndReset((ClassLoaderCallBack<IRedis>) () -> {
            ServiceLoader<IRedis> redis = ServiceLoader.load(IRedis.class);
            Iterator<IRedis> iClientIterator = redis.iterator();
            if (!iClientIterator.hasNext()) {
                throw new DtLoaderException("This plugin type is not supported: " + pluginName);
            }
            IRedis iRedis = iClientIterator.next();
            return new RedisProxy(iRedis);
        }, classLoader);
    }
}
