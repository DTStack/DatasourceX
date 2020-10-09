package com.dtstack.dtcenter.loader.client.mq;

import com.dtstack.dtcenter.loader.ClassLoaderCallBack;
import com.dtstack.dtcenter.loader.ClassLoaderCallBackMethod;
import com.dtstack.dtcenter.loader.client.ClientFactory;
import com.dtstack.dtcenter.loader.client.IKafka;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;

import java.util.Iterator;
import java.util.ServiceLoader;

/**
 * @company: www.dtstack.com
 * @Author ：wangchuan
 * @Date ：Created in 下午2:23 2020/6/2
 * @Description：kafka客户端工厂
 */
public class KafkaClientFactory {
    public static IKafka createPluginClass(String pluginName) throws Exception {
        ClassLoader classLoader = ClientFactory.getClassLoader(pluginName);
        return ClassLoaderCallBackMethod.callbackAndReset((ClassLoaderCallBack<IKafka>) () -> {
            ServiceLoader<IKafka> kafkas = ServiceLoader.load(IKafka.class);
            Iterator<IKafka> iClientIterator = kafkas.iterator();
            if (!iClientIterator.hasNext()) {
                throw new DtLoaderException("暂不支持该插件类型: " + pluginName);
            }

            IKafka kafka = iClientIterator.next();
            return new KafkaClientProxy(kafka);
        }, classLoader);
    }
}
