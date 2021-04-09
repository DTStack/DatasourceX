package com.dtstack.dtcenter.loader.client.table;

import com.dtstack.dtcenter.loader.ClassLoaderCallBack;
import com.dtstack.dtcenter.loader.ClassLoaderCallBackMethod;
import com.dtstack.dtcenter.loader.client.ClientFactory;
import com.dtstack.dtcenter.loader.client.ITable;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;

import java.util.Iterator;
import java.util.ServiceLoader;

/**
 * 表相关操作客户端工厂
 *
 * @author ：wangchuan
 * date：Created in 2:17 下午 2020/11/12
 * company: www.dtstack.com
 */
public class TableClientFactory {
    public static ITable createPluginClass(String pluginName) throws Exception {
        ClassLoader classLoader = ClientFactory.getClassLoader(pluginName);
        return ClassLoaderCallBackMethod.callbackAndReset((ClassLoaderCallBack<ITable>) () -> {
            ServiceLoader<ITable> tables = ServiceLoader.load(ITable.class);
            Iterator<ITable> iClientIterator = tables.iterator();
            if (!iClientIterator.hasNext()) {
                throw new DtLoaderException("This plugin type is not supported: " + pluginName);
            }

            ITable iTable = iClientIterator.next();
            return new TableClientProxy(iTable);
        }, classLoader);
    }
}
