package com.dtstack.dtcenter.loader.client.restful;

import com.dtstack.dtcenter.loader.ClassLoaderCallBack;
import com.dtstack.dtcenter.loader.ClassLoaderCallBackMethod;
import com.dtstack.dtcenter.loader.client.ClientFactory;
import com.dtstack.dtcenter.loader.client.IRestful;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;

import java.util.Iterator;
import java.util.ServiceLoader;

/**
 * <p> restful 客户端工厂</p>
 *
 * @author ：wangchuan
 * date：Created in 上午10:06 2021/8/11
 * company: www.dtstack.com
 */
public class RestfulClientFactory {
    public static IRestful createPluginClass(String pluginName) throws Exception {
        ClassLoader classLoader = ClientFactory.getClassLoader(pluginName);
        return ClassLoaderCallBackMethod.callbackAndReset((ClassLoaderCallBack<IRestful>) () -> {
            ServiceLoader<IRestful> restfuls = ServiceLoader.load(IRestful.class);
            Iterator<IRestful> iClientIterator = restfuls.iterator();
            if (!iClientIterator.hasNext()) {
                throw new DtLoaderException("This plugin type is not supported: " + pluginName);
            }
            IRestful restful = iClientIterator.next();
            return new RestfulClientProxy(restful);
        }, classLoader);
    }
}
