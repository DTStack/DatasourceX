package com.dtstack.dtcenter.loader.client.kerberos;

import com.dtstack.dtcenter.loader.ClassLoaderCallBack;
import com.dtstack.dtcenter.loader.ClassLoaderCallBackMethod;
import com.dtstack.dtcenter.loader.client.ClientFactory;
import com.dtstack.dtcenter.loader.client.IKerberos;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;

import java.util.Iterator;
import java.util.ServiceLoader;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 20:44 2020/8/26
 * @Description：Kerberos 认证客户端工厂
 */
public class KerberosClientFactory {
    public static IKerberos createPluginClass(String pluginName) throws Exception {
        ClassLoader classLoader = ClientFactory.getClassLoader(pluginName);
        return ClassLoaderCallBackMethod.callbackAndReset((ClassLoaderCallBack<IKerberos>) () -> {
            ServiceLoader<IKerberos> kerberos = ServiceLoader.load(IKerberos.class);
            Iterator<IKerberos> iClientIterator = kerberos.iterator();
            if (!iClientIterator.hasNext()) {
                throw new DtLoaderException("暂不支持该插件类型: " + pluginName);
            }

            IKerberos iKerberos = iClientIterator.next();
            return new KerberosProxy(iKerberos);
        }, classLoader);
    }
}
