package com.dtstack.dtcenter.loader.client.hdfs;

import com.dtstack.dtcenter.loader.ClassLoaderCallBack;
import com.dtstack.dtcenter.loader.ClassLoaderCallBackMethod;
import com.dtstack.dtcenter.loader.client.ClientFactory;
import com.dtstack.dtcenter.loader.client.IHdfsFile;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;

import java.util.Iterator;
import java.util.ServiceLoader;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 14:40 2020/8/10
 * @Description：Hdfs 文件操作客户端工厂
 */
public class HdfsFileClientFactory {

    public static IHdfsFile createPluginClass(String pluginName) throws Exception {
        ClassLoader classLoader = ClientFactory.getClassLoader(pluginName);
        return ClassLoaderCallBackMethod.callbackAndReset((ClassLoaderCallBack<IHdfsFile>) () -> {
            ServiceLoader<IHdfsFile> kafkas = ServiceLoader.load(IHdfsFile.class);
            Iterator<IHdfsFile> iClientIterator = kafkas.iterator();
            if (!iClientIterator.hasNext()) {
                throw new DtLoaderException("暂不支持该插件类型: " + pluginName);
            }

            IHdfsFile hdfsFile = iClientIterator.next();
            return new HdfsFileProxy(hdfsFile);
        }, classLoader, true);
    }
}
