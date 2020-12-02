package com.dtstack.dtcenter.loader.client.hbase;

import com.dtstack.dtcenter.loader.ClassLoaderCallBack;
import com.dtstack.dtcenter.loader.ClassLoaderCallBackMethod;
import com.dtstack.dtcenter.loader.client.ClientFactory;
import com.dtstack.dtcenter.loader.client.IHbase;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;

import java.util.Iterator;
import java.util.ServiceLoader;

/**
 * hbase客户端工厂
 *
 * @author ：wangchuan
 * date：Created in 9:38 上午 2020/12/2
 * company: www.dtstack.com
 */
public class HbaseClientFactory {
    public static IHbase createPluginClass(String pluginName) throws Exception {
        ClassLoader classLoader = ClientFactory.getClassLoader(pluginName);
        return ClassLoaderCallBackMethod.callbackAndReset((ClassLoaderCallBack<IHbase>) () -> {
            ServiceLoader<IHbase> hbases = ServiceLoader.load(IHbase.class);
            Iterator<IHbase> iClientIterator = hbases.iterator();
            if (!iClientIterator.hasNext()) {
                throw new DtLoaderException("暂不支持该插件类型: " + pluginName);
            }
            IHbase hbase = iClientIterator.next();
            return new HbaseProxy(hbase);
        }, classLoader);
    }
}
