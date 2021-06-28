package com.dtstack.dtcenter.loader.client.tsdb;

import com.dtstack.dtcenter.loader.ClassLoaderCallBack;
import com.dtstack.dtcenter.loader.ClassLoaderCallBackMethod;
import com.dtstack.dtcenter.loader.client.ClientFactory;
import com.dtstack.dtcenter.loader.client.ITsdb;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;

import java.util.Iterator;
import java.util.ServiceLoader;

/**
 * <p> OpenTSDB 客户端工厂</p>
 *
 * @author ：wangchuan
 * date：Created in 上午10:06 2021/6/23
 * company: www.dtstack.com
 */
public class TsdbClientFactory {
    public static ITsdb createPluginClass(String pluginName) throws Exception {
        ClassLoader classLoader = ClientFactory.getClassLoader(pluginName);
        return ClassLoaderCallBackMethod.callbackAndReset((ClassLoaderCallBack<ITsdb>) () -> {
            ServiceLoader<ITsdb> tsdbs = ServiceLoader.load(ITsdb.class);
            Iterator<ITsdb> iClientIterator = tsdbs.iterator();
            if (!iClientIterator.hasNext()) {
                throw new DtLoaderException("This plugin type is not supported: " + pluginName);
            }
            ITsdb iTsdb = iClientIterator.next();
            return new TsdbClientProxy(iTsdb);
        }, classLoader);
    }
}
