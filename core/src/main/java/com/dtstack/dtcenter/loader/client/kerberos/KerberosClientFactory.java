package com.dtstack.dtcenter.loader.client.kerberos;

import com.dtstack.dtcenter.loader.ClassLoaderCallBack;
import com.dtstack.dtcenter.loader.ClassLoaderCallBackMethod;
import com.dtstack.dtcenter.loader.client.ClientFactory;
import com.dtstack.dtcenter.loader.client.IKerberos;

import java.util.Iterator;
import java.util.ServiceLoader;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 20:44 2020/8/26
 * @Description：Kerberos 认证客户端工厂
 */
public class KerberosClientFactory {
    /**
     * Kerberos 插件包名称
     */
    private static final String KERBEROS_PLUGIN_NAME = "kerberos";

    public static IKerberos createPluginClass() throws Exception {
        ClassLoader classLoader = ClientFactory.getClassLoader(KERBEROS_PLUGIN_NAME);
        return ClassLoaderCallBackMethod.callbackAndReset((ClassLoaderCallBack<IKerberos>) () -> {
            ServiceLoader<IKerberos> kerberos = ServiceLoader.load(IKerberos.class);
            Iterator<IKerberos> iClientIterator = kerberos.iterator();
            if (!iClientIterator.hasNext()) {
                throw new RuntimeException("暂不支持该插件类型: " + KERBEROS_PLUGIN_NAME);
            }

            IKerberos iKerberos = iClientIterator.next();
            return new KerberosProxy(iKerberos);
        }, classLoader, true);
    }
}
